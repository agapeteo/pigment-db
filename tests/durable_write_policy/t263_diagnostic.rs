//! Counterbalanced execution protocol for the focused T263 diagnostic.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticVariant {
    Baseline,
    Candidate,
}

pub fn run_alternating_pairs(
    pair_count: usize,
    mut run: impl FnMut(usize, usize, DiagnosticVariant),
) {
    for pair_index in 0..pair_count {
        let variants = if pair_index % 2 == 0 {
            [DiagnosticVariant::Baseline, DiagnosticVariant::Candidate]
        } else {
            [DiagnosticVariant::Candidate, DiagnosticVariant::Baseline]
        };
        for (position, variant) in variants.into_iter().enumerate() {
            run(pair_index, position, variant);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alternating_pairs_counterbalance_order_without_dropping_either_variant() {
        let mut observed = Vec::new();

        run_alternating_pairs(4, |pair_index, position, variant| {
            observed.push((pair_index, position, variant));
        });

        assert_eq!(
            observed,
            vec![
                (0, 0, DiagnosticVariant::Baseline),
                (0, 1, DiagnosticVariant::Candidate),
                (1, 0, DiagnosticVariant::Candidate),
                (1, 1, DiagnosticVariant::Baseline),
                (2, 0, DiagnosticVariant::Baseline),
                (2, 1, DiagnosticVariant::Candidate),
                (3, 0, DiagnosticVariant::Candidate),
                (3, 1, DiagnosticVariant::Baseline),
            ]
        );
    }
}
