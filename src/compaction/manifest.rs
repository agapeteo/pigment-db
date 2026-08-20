//! Temporary compaction-manifest internals.

#![allow(dead_code)]

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::path::Component;
use std::path::{Path, PathBuf};

use crate::{DurabilityPolicy, StoreFamily};

const MANIFEST_MAGIC: [u8; 8] = *b"PIGCMP\r\n";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_LEN: usize = 16;
const MANIFEST_VERSION_OFFSET: usize = 8;
const MANIFEST_BODY_LEN_OFFSET: usize = 12;
const MAX_MANIFEST_BODY_LEN: usize = 1024 * 1024;
const MAX_MANIFEST_PATH_LEN: usize = 4096;
const MAX_MANIFEST_DESCRIPTORS: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestMode {
    ClosedDirectory,
    OnlineFamily,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ManifestScope {
    Directory,
    Family {
        family: StoreFamily,
        active_name: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestPhase {
    Prepared,
    PreviousPublished,
    ReplacementPublished,
    CleanupPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactRole {
    Active,
    SealedSegment,
    Staging,
    PreviousGeneration,
    ReplacementPrefix,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactDescriptor {
    pub(crate) relative_path: PathBuf,
    pub(crate) role: ArtifactRole,
    pub(crate) family: Option<StoreFamily>,
    pub(crate) length: u64,
    pub(crate) checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionManifest {
    pub(crate) operation_id: [u8; 16],
    pub(crate) mode: ManifestMode,
    pub(crate) scope: ManifestScope,
    pub(crate) phase: ManifestPhase,
    pub(crate) source_finalized: bool,
    pub(crate) durability: DurabilityPolicy,
    pub(crate) source_inventory: Vec<ArtifactDescriptor>,
    pub(crate) staging_location: PathBuf,
    pub(crate) previous_location: PathBuf,
    pub(crate) replacement_inventory: Vec<ArtifactDescriptor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestCodecError {
    InvalidEnvelope,
    UnsupportedVersion,
    BodyTooLarge,
    ChecksumMismatch,
    InvalidBody,
    InvalidPath,
    DuplicatePath,
    LimitExceeded,
    DescriptorMismatch,
}

pub(crate) fn verify_descriptor(
    anchor: &Path,
    descriptor: &ArtifactDescriptor,
) -> Result<(), ManifestCodecError> {
    validate_relative_path(&descriptor.relative_path)?;
    let canonical_anchor =
        fs::canonicalize(anchor).map_err(|_| ManifestCodecError::DescriptorMismatch)?;
    let mut artifact = anchor.to_path_buf();
    for component in descriptor.relative_path.components() {
        let Component::Normal(component) = component else {
            return Err(ManifestCodecError::InvalidPath);
        };
        artifact.push(component);
        let metadata =
            fs::symlink_metadata(&artifact).map_err(|_| ManifestCodecError::DescriptorMismatch)?;
        if metadata.file_type().is_symlink() {
            return Err(ManifestCodecError::InvalidPath);
        }
    }
    let canonical_artifact =
        fs::canonicalize(&artifact).map_err(|_| ManifestCodecError::DescriptorMismatch)?;
    if !canonical_artifact.starts_with(&canonical_anchor) {
        return Err(ManifestCodecError::InvalidPath);
    }
    let metadata =
        fs::metadata(&canonical_artifact).map_err(|_| ManifestCodecError::DescriptorMismatch)?;
    if !metadata.is_file() || metadata.len() != descriptor.length {
        return Err(ManifestCodecError::DescriptorMismatch);
    }
    let mut file =
        fs::File::open(&canonical_artifact).map_err(|_| ManifestCodecError::DescriptorMismatch)?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| ManifestCodecError::DescriptorMismatch)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    if hasher.finalize() != descriptor.checksum {
        return Err(ManifestCodecError::DescriptorMismatch);
    }
    Ok(())
}

pub(crate) fn encode_manifest(
    manifest: &CompactionManifest,
) -> Result<Vec<u8>, ManifestCodecError> {
    validate_manifest(manifest)?;
    let mut body = BodyWriter::default();
    body.bytes(&manifest.operation_id);
    body.byte(match manifest.mode {
        ManifestMode::ClosedDirectory => 1,
        ManifestMode::OnlineFamily => 2,
    });
    match &manifest.scope {
        ManifestScope::Directory => body.byte(1),
        ManifestScope::Family {
            family,
            active_name,
        } => {
            body.byte(2);
            body.byte(encode_family(*family));
            body.path(active_name)?;
        }
    }
    body.byte(match manifest.phase {
        ManifestPhase::Prepared => 1,
        ManifestPhase::PreviousPublished => 2,
        ManifestPhase::ReplacementPublished => 3,
        ManifestPhase::CleanupPending => 4,
    });
    body.byte(u8::from(manifest.source_finalized));
    body.byte(match manifest.durability {
        DurabilityPolicy::Buffered => 1,
        DurabilityPolicy::Physical => 2,
    });
    body.descriptors(&manifest.source_inventory)?;
    body.path(&manifest.staging_location)?;
    body.path(&manifest.previous_location)?;
    body.descriptors(&manifest.replacement_inventory)?;

    if body.encoded.len() > MAX_MANIFEST_BODY_LEN {
        return Err(ManifestCodecError::BodyTooLarge);
    }
    let body_len =
        u32::try_from(body.encoded.len()).map_err(|_| ManifestCodecError::BodyTooLarge)?;
    let total_without_crc = MANIFEST_HEADER_LEN
        .checked_add(body.encoded.len())
        .ok_or(ManifestCodecError::BodyTooLarge)?;
    let total = total_without_crc
        .checked_add(std::mem::size_of::<u32>())
        .ok_or(ManifestCodecError::BodyTooLarge)?;
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(&MANIFEST_MAGIC);
    encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0_u16.to_le_bytes());
    encoded.extend_from_slice(&body_len.to_le_bytes());
    encoded.extend_from_slice(&body.encoded);
    let checksum = crc32fast::hash(&encoded);
    encoded.extend_from_slice(&checksum.to_le_bytes());
    Ok(encoded)
}

pub(crate) fn decode_manifest(encoded: &[u8]) -> Result<CompactionManifest, ManifestCodecError> {
    let minimum_len = MANIFEST_HEADER_LEN
        .checked_add(std::mem::size_of::<u32>())
        .ok_or(ManifestCodecError::InvalidEnvelope)?;
    if encoded.len() < minimum_len || encoded.get(..8) != Some(MANIFEST_MAGIC.as_slice()) {
        return Err(ManifestCodecError::InvalidEnvelope);
    }
    let version = read_u16(encoded, MANIFEST_VERSION_OFFSET)?;
    if version != MANIFEST_VERSION {
        return Err(ManifestCodecError::UnsupportedVersion);
    }
    if read_u16(encoded, 10)? != 0 {
        return Err(ManifestCodecError::InvalidEnvelope);
    }
    let body_len = usize::try_from(read_u32(encoded, MANIFEST_BODY_LEN_OFFSET)?)
        .map_err(|_| ManifestCodecError::BodyTooLarge)?;
    if body_len > MAX_MANIFEST_BODY_LEN {
        return Err(ManifestCodecError::BodyTooLarge);
    }
    let checksum_start = MANIFEST_HEADER_LEN
        .checked_add(body_len)
        .ok_or(ManifestCodecError::BodyTooLarge)?;
    let expected_len = checksum_start
        .checked_add(std::mem::size_of::<u32>())
        .ok_or(ManifestCodecError::BodyTooLarge)?;
    if encoded.len() != expected_len {
        return Err(ManifestCodecError::InvalidEnvelope);
    }
    let stored_checksum = read_u32(encoded, checksum_start)?;
    if stored_checksum != crc32fast::hash(&encoded[..checksum_start]) {
        return Err(ManifestCodecError::ChecksumMismatch);
    }

    let mut body = BodyReader::new(&encoded[MANIFEST_HEADER_LEN..checksum_start]);
    let operation_id = body.array_16()?;
    let mode = match body.byte()? {
        1 => ManifestMode::ClosedDirectory,
        2 => ManifestMode::OnlineFamily,
        _ => return Err(ManifestCodecError::InvalidBody),
    };
    let scope = match body.byte()? {
        1 => ManifestScope::Directory,
        2 => ManifestScope::Family {
            family: decode_family(body.byte()?)?,
            active_name: body.path()?,
        },
        _ => return Err(ManifestCodecError::InvalidBody),
    };
    let phase = match body.byte()? {
        1 => ManifestPhase::Prepared,
        2 => ManifestPhase::PreviousPublished,
        3 => ManifestPhase::ReplacementPublished,
        4 => ManifestPhase::CleanupPending,
        _ => return Err(ManifestCodecError::InvalidBody),
    };
    let source_finalized = match body.byte()? {
        0 => false,
        1 => true,
        _ => return Err(ManifestCodecError::InvalidBody),
    };
    let durability = match body.byte()? {
        1 => DurabilityPolicy::Buffered,
        2 => DurabilityPolicy::Physical,
        _ => return Err(ManifestCodecError::InvalidBody),
    };
    let source_inventory = body.descriptors()?;
    let staging_location = body.path()?;
    let previous_location = body.path()?;
    let replacement_inventory = body.descriptors()?;
    if !body.is_finished() {
        return Err(ManifestCodecError::InvalidBody);
    }
    let manifest = CompactionManifest {
        operation_id,
        mode,
        scope,
        phase,
        source_finalized,
        durability,
        source_inventory,
        staging_location,
        previous_location,
        replacement_inventory,
    };
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &CompactionManifest) -> Result<(), ManifestCodecError> {
    match (&manifest.mode, &manifest.scope) {
        (ManifestMode::ClosedDirectory, ManifestScope::Directory)
        | (ManifestMode::OnlineFamily, ManifestScope::Family { .. }) => {}
        _ => return Err(ManifestCodecError::InvalidBody),
    }
    if manifest.mode == ManifestMode::ClosedDirectory && !manifest.source_finalized {
        return Err(ManifestCodecError::InvalidBody);
    }
    if let ManifestScope::Family { active_name, .. } = &manifest.scope {
        validate_relative_path(active_name)?;
    }
    validate_relative_path(&manifest.staging_location)?;
    validate_relative_path(&manifest.previous_location)?;
    if manifest.staging_location == manifest.previous_location {
        return Err(ManifestCodecError::DuplicatePath);
    }
    validate_descriptors(&manifest.source_inventory)?;
    validate_descriptors(&manifest.replacement_inventory)?;
    Ok(())
}

fn validate_descriptors(descriptors: &[ArtifactDescriptor]) -> Result<(), ManifestCodecError> {
    if descriptors.len() > MAX_MANIFEST_DESCRIPTORS {
        return Err(ManifestCodecError::LimitExceeded);
    }
    let mut paths = HashSet::with_capacity(descriptors.len());
    for descriptor in descriptors {
        validate_relative_path(&descriptor.relative_path)?;
        if !paths.insert(descriptor.relative_path.clone()) {
            return Err(ManifestCodecError::DuplicatePath);
        }
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), ManifestCodecError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(ManifestCodecError::InvalidPath);
    }
    let native = encode_native_path(path)?;
    if native.len() > MAX_MANIFEST_PATH_LEN {
        return Err(ManifestCodecError::LimitExceeded);
    }
    let mut canonical = PathBuf::new();
    let mut component_count = 0_usize;
    for component in path.components() {
        let Component::Normal(component) = component else {
            return Err(ManifestCodecError::InvalidPath);
        };
        canonical.push(component);
        component_count = component_count
            .checked_add(1)
            .ok_or(ManifestCodecError::LimitExceeded)?;
    }
    if component_count == 0 || canonical.as_os_str() != path.as_os_str() {
        return Err(ManifestCodecError::InvalidPath);
    }
    Ok(())
}

fn encode_family(family: StoreFamily) -> u8 {
    match family {
        StoreFamily::KeyValue => 1,
        StoreFamily::KeySet => 2,
        StoreFamily::KeyMap => 3,
    }
}

fn decode_family(encoded: u8) -> Result<StoreFamily, ManifestCodecError> {
    match encoded {
        1 => Ok(StoreFamily::KeyValue),
        2 => Ok(StoreFamily::KeySet),
        3 => Ok(StoreFamily::KeyMap),
        _ => Err(ManifestCodecError::InvalidBody),
    }
}

fn read_u16(encoded: &[u8], offset: usize) -> Result<u16, ManifestCodecError> {
    encoded
        .get(offset..offset + 2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or(ManifestCodecError::InvalidEnvelope)
}

fn read_u32(encoded: &[u8], offset: usize) -> Result<u32, ManifestCodecError> {
    encoded
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(ManifestCodecError::InvalidEnvelope)
}

#[derive(Default)]
struct BodyWriter {
    encoded: Vec<u8>,
}

impl BodyWriter {
    fn byte(&mut self, value: u8) {
        self.encoded.push(value);
    }

    fn bytes(&mut self, value: &[u8]) {
        self.encoded.extend_from_slice(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }

    fn path(&mut self, path: &Path) -> Result<(), ManifestCodecError> {
        let native = encode_native_path(path)?;
        if native.len() > MAX_MANIFEST_PATH_LEN {
            return Err(ManifestCodecError::LimitExceeded);
        }
        let length = u32::try_from(native.len()).map_err(|_| ManifestCodecError::BodyTooLarge)?;
        self.u32(length);
        self.bytes(&native);
        Ok(())
    }

    fn descriptor(&mut self, descriptor: &ArtifactDescriptor) -> Result<(), ManifestCodecError> {
        self.path(&descriptor.relative_path)?;
        self.byte(match descriptor.role {
            ArtifactRole::Active => 1,
            ArtifactRole::SealedSegment => 2,
            ArtifactRole::Staging => 3,
            ArtifactRole::PreviousGeneration => 4,
            ArtifactRole::ReplacementPrefix => 5,
        });
        self.byte(descriptor.family.map_or(0, encode_family));
        self.u64(descriptor.length);
        self.u32(descriptor.checksum);
        Ok(())
    }

    fn descriptors(
        &mut self,
        descriptors: &[ArtifactDescriptor],
    ) -> Result<(), ManifestCodecError> {
        if descriptors.len() > MAX_MANIFEST_DESCRIPTORS {
            return Err(ManifestCodecError::LimitExceeded);
        }
        let count =
            u32::try_from(descriptors.len()).map_err(|_| ManifestCodecError::BodyTooLarge)?;
        self.u32(count);
        for descriptor in descriptors {
            self.descriptor(descriptor)?;
        }
        Ok(())
    }
}

struct BodyReader<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> BodyReader<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], ManifestCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ManifestCodecError::InvalidBody)?;
        let bytes = self
            .encoded
            .get(self.offset..end)
            .ok_or(ManifestCodecError::InvalidBody)?;
        self.offset = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, ManifestCodecError> {
        self.bytes(1).map(|bytes| bytes[0])
    }

    fn u32(&mut self) -> Result<u32, ManifestCodecError> {
        self.bytes(4)?
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| ManifestCodecError::InvalidBody)
    }

    fn u64(&mut self) -> Result<u64, ManifestCodecError> {
        self.bytes(8)?
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| ManifestCodecError::InvalidBody)
    }

    fn array_16(&mut self) -> Result<[u8; 16], ManifestCodecError> {
        self.bytes(16)?
            .try_into()
            .map_err(|_| ManifestCodecError::InvalidBody)
    }

    fn path(&mut self) -> Result<PathBuf, ManifestCodecError> {
        let length = usize::try_from(self.u32()?).map_err(|_| ManifestCodecError::InvalidBody)?;
        if length > MAX_MANIFEST_PATH_LEN {
            return Err(ManifestCodecError::LimitExceeded);
        }
        decode_native_path(self.bytes(length)?)
    }

    fn descriptor(&mut self) -> Result<ArtifactDescriptor, ManifestCodecError> {
        let relative_path = self.path()?;
        let role = match self.byte()? {
            1 => ArtifactRole::Active,
            2 => ArtifactRole::SealedSegment,
            3 => ArtifactRole::Staging,
            4 => ArtifactRole::PreviousGeneration,
            5 => ArtifactRole::ReplacementPrefix,
            _ => return Err(ManifestCodecError::InvalidBody),
        };
        let family = match self.byte()? {
            0 => None,
            encoded => Some(decode_family(encoded)?),
        };
        Ok(ArtifactDescriptor {
            relative_path,
            role,
            family,
            length: self.u64()?,
            checksum: self.u32()?,
        })
    }

    fn descriptors(&mut self) -> Result<Vec<ArtifactDescriptor>, ManifestCodecError> {
        let count = usize::try_from(self.u32()?).map_err(|_| ManifestCodecError::InvalidBody)?;
        if count > MAX_MANIFEST_DESCRIPTORS {
            return Err(ManifestCodecError::LimitExceeded);
        }
        let mut descriptors = Vec::with_capacity(count);
        for _ in 0..count {
            descriptors.push(self.descriptor()?);
        }
        Ok(descriptors)
    }

    fn is_finished(&self) -> bool {
        self.offset == self.encoded.len()
    }
}

#[cfg(unix)]
fn encode_native_path(path: &Path) -> Result<Vec<u8>, ManifestCodecError> {
    use std::os::unix::ffi::OsStrExt;

    Ok(path.as_os_str().as_bytes().to_vec())
}

#[cfg(unix)]
fn decode_native_path(encoded: &[u8]) -> Result<PathBuf, ManifestCodecError> {
    use std::os::unix::ffi::OsStringExt;

    Ok(PathBuf::from(OsString::from_vec(encoded.to_vec())))
}

#[cfg(windows)]
fn encode_native_path(path: &Path) -> Result<Vec<u8>, ManifestCodecError> {
    use std::os::windows::ffi::OsStrExt;

    let mut encoded = Vec::new();
    for unit in path.as_os_str().encode_wide() {
        encoded.extend_from_slice(&unit.to_le_bytes());
    }
    Ok(encoded)
}

#[cfg(windows)]
fn decode_native_path(encoded: &[u8]) -> Result<PathBuf, ManifestCodecError> {
    use std::os::windows::ffi::OsStringExt;

    let mut units = Vec::with_capacity(encoded.len() / 2);
    let mut chunks = encoded.chunks_exact(2);
    for chunk in &mut chunks {
        units.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    if !chunks.remainder().is_empty() {
        return Err(ManifestCodecError::InvalidBody);
    }
    Ok(PathBuf::from(OsString::from_wide(&units)))
}

#[cfg(not(any(unix, windows)))]
fn encode_native_path(path: &Path) -> Result<Vec<u8>, ManifestCodecError> {
    path.to_str()
        .map(|path| path.as_bytes().to_vec())
        .ok_or(ManifestCodecError::InvalidBody)
}

#[cfg(not(any(unix, windows)))]
fn decode_native_path(encoded: &[u8]) -> Result<PathBuf, ManifestCodecError> {
    String::from_utf8(encoded.to_vec())
        .map(PathBuf::from)
        .map_err(|_| ManifestCodecError::InvalidBody)
}

#[cfg(test)]
pub(crate) fn test_sentinel() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::publication::{
        directory_artifact_paths, family_artifact_paths, publish_manifest_buffered,
        publish_manifest_buffered_with_checkpoint, read_published_manifest, ManifestPublishStage,
    };

    fn rechecksum(encoded: &mut [u8]) {
        let checksum_start = encoded.len() - std::mem::size_of::<u32>();
        let checksum = crc32fast::hash(&encoded[..checksum_start]);
        encoded[checksum_start..].copy_from_slice(&checksum.to_le_bytes());
    }

    fn descriptor(name: &str, role: ArtifactRole) -> ArtifactDescriptor {
        ArtifactDescriptor {
            relative_path: PathBuf::from(name),
            role,
            family: Some(StoreFamily::KeyValue),
            length: 123,
            checksum: 0x1234_5678,
        }
    }

    fn manifest(
        scope: ManifestScope,
        phase: ManifestPhase,
        durability: DurabilityPolicy,
    ) -> CompactionManifest {
        let mode = match &scope {
            ManifestScope::Directory => ManifestMode::ClosedDirectory,
            ManifestScope::Family { .. } => ManifestMode::OnlineFamily,
        };
        CompactionManifest {
            operation_id: *b"0123456789abcdef",
            mode,
            scope,
            phase,
            source_finalized: mode == ManifestMode::ClosedDirectory,
            durability,
            source_inventory: vec![descriptor("source.pigment", ArtifactRole::Active)],
            staging_location: PathBuf::from("staging.pigment"),
            previous_location: PathBuf::from("previous.pigment"),
            replacement_inventory: vec![descriptor(
                "replacement.pigment",
                ArtifactRole::ReplacementPrefix,
            )],
        }
    }

    #[test]
    fn manifest_roundtrips_every_scope_phase_and_policy_without_application_payload() {
        let scopes = [
            ManifestScope::Directory,
            ManifestScope::Family {
                family: StoreFamily::KeyValue,
                active_name: PathBuf::from("key_value_store"),
            },
            ManifestScope::Family {
                family: StoreFamily::KeySet,
                active_name: PathBuf::from("key_set_store"),
            },
            ManifestScope::Family {
                family: StoreFamily::KeyMap,
                active_name: PathBuf::from("key_map_store"),
            },
        ];
        let phases = [
            ManifestPhase::Prepared,
            ManifestPhase::PreviousPublished,
            ManifestPhase::ReplacementPublished,
            ManifestPhase::CleanupPending,
        ];
        for scope in scopes {
            for phase in phases {
                for durability in [DurabilityPolicy::Buffered, DurabilityPolicy::Physical] {
                    let expected = manifest(scope.clone(), phase, durability);
                    let encoded = encode_manifest(&expected).unwrap();
                    assert_eq!(&encoded[..MANIFEST_MAGIC.len()], &MANIFEST_MAGIC);
                    assert_eq!(decode_manifest(&encoded).unwrap(), expected);
                    assert!(!encoded
                        .windows(b"secret-application-value".len())
                        .any(|window| window == b"secret-application-value"));
                }
            }
        }
    }

    #[test]
    fn manifest_rejects_invalid_envelope_version_body_bounds_and_crc() {
        let encoded = encode_manifest(&manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        ))
        .unwrap();

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 0xff;
        assert_eq!(
            decode_manifest(&bad_magic),
            Err(ManifestCodecError::InvalidEnvelope)
        );

        let mut bad_version = encoded.clone();
        bad_version[MANIFEST_VERSION_OFFSET..MANIFEST_VERSION_OFFSET + 2]
            .copy_from_slice(&(MANIFEST_VERSION + 1).to_le_bytes());
        assert_eq!(
            decode_manifest(&bad_version),
            Err(ManifestCodecError::UnsupportedVersion)
        );

        let mut excessive_body = encoded.clone();
        excessive_body[MANIFEST_BODY_LEN_OFFSET..MANIFEST_BODY_LEN_OFFSET + 4].copy_from_slice(
            &u32::try_from(MAX_MANIFEST_BODY_LEN + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert_eq!(
            decode_manifest(&excessive_body),
            Err(ManifestCodecError::BodyTooLarge)
        );

        assert_eq!(
            decode_manifest(&encoded[..MANIFEST_HEADER_LEN - 1]),
            Err(ManifestCodecError::InvalidEnvelope)
        );

        let mut bad_crc = encoded;
        bad_crc[MANIFEST_HEADER_LEN] ^= 0xff;
        assert_eq!(
            decode_manifest(&bad_crc),
            Err(ManifestCodecError::ChecksumMismatch)
        );
    }

    #[test]
    fn manifest_rejects_noncanonical_escaping_and_duplicate_native_paths() {
        let absolute = std::env::current_dir().unwrap().join("absolute-artifact");
        let parent = PathBuf::from("source").join("..").join("escaped");
        let alias = PathBuf::from(format!(
            "source{separator}.{separator}artifact",
            separator = std::path::MAIN_SEPARATOR
        ));
        for invalid in [absolute, parent, alias] {
            let mut invalid_manifest = manifest(
                ManifestScope::Directory,
                ManifestPhase::Prepared,
                DurabilityPolicy::Buffered,
            );
            invalid_manifest.source_inventory[0].relative_path = invalid;
            assert_eq!(
                encode_manifest(&invalid_manifest),
                Err(ManifestCodecError::InvalidPath)
            );
        }

        let mut duplicate = manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        );
        duplicate
            .source_inventory
            .push(duplicate.source_inventory[0].clone());
        assert_eq!(
            encode_manifest(&duplicate),
            Err(ManifestCodecError::DuplicatePath)
        );

        let mut invalid_location = manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        );
        invalid_location.staging_location = PathBuf::from("..").join("staging");
        assert_eq!(
            encode_manifest(&invalid_location),
            Err(ManifestCodecError::InvalidPath)
        );
    }

    #[test]
    fn manifest_rejects_excessive_lengths_counts_and_unknown_enum_values_before_allocation() {
        let mut excessive_path = manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        );
        excessive_path.source_inventory[0].relative_path =
            PathBuf::from("a".repeat(MAX_MANIFEST_PATH_LEN + 1));
        assert_eq!(
            encode_manifest(&excessive_path),
            Err(ManifestCodecError::LimitExceeded)
        );

        let mut excessive_count = encode_manifest(&manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        ))
        .unwrap();
        let source_count_offset = MANIFEST_HEADER_LEN + 21;
        excessive_count[source_count_offset..source_count_offset + 4].copy_from_slice(
            &u32::try_from(MAX_MANIFEST_DESCRIPTORS + 1)
                .unwrap()
                .to_le_bytes(),
        );
        rechecksum(&mut excessive_count);
        assert_eq!(
            decode_manifest(&excessive_count),
            Err(ManifestCodecError::LimitExceeded)
        );

        for body_offset in [16_usize, 17, 18, 19, 20] {
            let mut unknown = encode_manifest(&manifest(
                ManifestScope::Directory,
                ManifestPhase::Prepared,
                DurabilityPolicy::Buffered,
            ))
            .unwrap();
            unknown[MANIFEST_HEADER_LEN + body_offset] = 0xff;
            rechecksum(&mut unknown);
            assert_eq!(
                decode_manifest(&unknown),
                Err(ManifestCodecError::InvalidBody)
            );
        }

        let mut unknown_descriptor = encode_manifest(&manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        ))
        .unwrap();
        let source_path_len = "source.pigment".len();
        let role_offset = MANIFEST_HEADER_LEN + 25 + 4 + source_path_len;
        unknown_descriptor[role_offset] = 0xff;
        rechecksum(&mut unknown_descriptor);
        assert_eq!(
            decode_manifest(&unknown_descriptor),
            Err(ManifestCodecError::InvalidBody)
        );
    }

    #[test]
    fn descriptor_verification_is_anchor_bounded_and_matches_exact_length_and_checksum() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"exact artifact bytes";
        std::fs::write(directory.path().join("artifact"), content).unwrap();
        let exact = ArtifactDescriptor {
            relative_path: PathBuf::from("artifact"),
            role: ArtifactRole::Active,
            family: Some(StoreFamily::KeyValue),
            length: u64::try_from(content.len()).unwrap(),
            checksum: crc32fast::hash(content),
        };
        assert_eq!(verify_descriptor(directory.path(), &exact), Ok(()));

        let mut wrong_length = exact.clone();
        wrong_length.length += 1;
        assert_eq!(
            verify_descriptor(directory.path(), &wrong_length),
            Err(ManifestCodecError::DescriptorMismatch)
        );
        let mut wrong_checksum = exact;
        wrong_checksum.checksum ^= 1;
        assert_eq!(
            verify_descriptor(directory.path(), &wrong_checksum),
            Err(ManifestCodecError::DescriptorMismatch)
        );
        assert_eq!(
            verify_descriptor(
                directory.path(),
                &ArtifactDescriptor {
                    relative_path: PathBuf::from("..").join("escaped"),
                    role: ArtifactRole::Active,
                    family: Some(StoreFamily::KeyValue),
                    length: 0,
                    checksum: 0,
                }
            ),
            Err(ManifestCodecError::InvalidPath)
        );
    }

    #[test]
    fn buffered_manifest_publication_writes_flushes_then_renames_and_main_wins() {
        let parent = tempfile::tempdir().unwrap();
        let store_dir = parent.path().join("database");
        std::fs::create_dir(&store_dir).unwrap();
        let paths = directory_artifact_paths(&store_dir).unwrap();
        let prepared = manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        );
        let mut stages = Vec::new();
        publish_manifest_buffered_with_checkpoint(&paths, &prepared, |stage| {
            stages.push(stage);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            stages,
            [
                ManifestPublishStage::Created,
                ManifestPublishStage::Written,
                ManifestPublishStage::Flushed,
                ManifestPublishStage::Renamed,
            ]
        );
        assert_eq!(
            read_published_manifest(&paths).unwrap(),
            Some(prepared.clone())
        );
        assert!(!paths.manifest_next.exists());

        let mut unpublished = prepared.clone();
        unpublished.phase = ManifestPhase::CleanupPending;
        std::fs::write(&paths.manifest_next, encode_manifest(&unpublished).unwrap()).unwrap();
        assert_eq!(read_published_manifest(&paths).unwrap(), Some(prepared));
    }

    #[test]
    fn failed_temp_publication_preserves_main_phase_and_unpublished_evidence() {
        let parent = tempfile::tempdir().unwrap();
        let store_dir = parent.path().join("database");
        std::fs::create_dir(&store_dir).unwrap();
        let paths = directory_artifact_paths(&store_dir).unwrap();
        let prepared = manifest(
            ManifestScope::Directory,
            ManifestPhase::Prepared,
            DurabilityPolicy::Buffered,
        );
        publish_manifest_buffered(&paths, &prepared).unwrap();

        let mut next = prepared.clone();
        next.phase = ManifestPhase::PreviousPublished;
        let failure = publish_manifest_buffered_with_checkpoint(&paths, &next, |stage| {
            if stage == ManifestPublishStage::Flushed {
                Err(std::io::Error::other("injected pre-rename failure"))
            } else {
                Ok(())
            }
        });
        assert!(failure.is_err());
        assert_eq!(read_published_manifest(&paths).unwrap(), Some(prepared));
        assert_eq!(
            decode_manifest(&std::fs::read(&paths.manifest_next).unwrap()).unwrap(),
            next
        );
        assert!(publish_manifest_buffered(&paths, &next).is_err());
    }

    #[test]
    fn maintenance_artifact_names_append_to_native_directory_and_family_names() {
        let parent = tempfile::tempdir().unwrap();
        let store_dir = parent.path().join("dâtabase");
        let directory = directory_artifact_paths(&store_dir).unwrap();
        assert_eq!(
            directory.manifest.file_name().unwrap(),
            ".dâtabase.pigment-compact.manifest"
        );
        assert_eq!(
            directory.manifest_next.file_name().unwrap(),
            ".dâtabase.pigment-compact.manifest.next"
        );
        assert_eq!(
            directory.staging.file_name().unwrap(),
            ".dâtabase.pigment-compact.next"
        );
        assert_eq!(
            directory.previous.file_name().unwrap(),
            ".dâtabase.pigment-compact.previous"
        );

        let family = family_artifact_paths(&store_dir.join("key_value_store")).unwrap();
        assert_eq!(
            family.manifest.file_name().unwrap(),
            "key_value_store.pigment-compact.manifest"
        );
        assert_eq!(family.manifest.parent(), Some(store_dir.as_path()));
        assert_eq!(family.manifest_next.parent(), Some(store_dir.as_path()));
        assert_eq!(family.staging.parent(), Some(store_dir.as_path()));
        assert_eq!(family.previous.parent(), Some(store_dir.as_path()));
    }
}
