//! Temporary compaction-manifest internals.

#![allow(dead_code)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::{DurabilityPolicy, StoreFamily};

const MANIFEST_MAGIC: [u8; 8] = *b"PIGCMP\r\n";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_LEN: usize = 16;
const MANIFEST_VERSION_OFFSET: usize = 8;
const MANIFEST_BODY_LEN_OFFSET: usize = 12;
const MAX_MANIFEST_BODY_LEN: usize = 1024 * 1024;

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
}

pub(crate) fn encode_manifest(
    manifest: &CompactionManifest,
) -> Result<Vec<u8>, ManifestCodecError> {
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
    Ok(CompactionManifest {
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
    })
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
}
