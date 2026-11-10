use crate::doctor::raw_mirror::doctor_bound_healthy_listing::doctor_bound_healthy_listing;

#[test]
pub(crate) fn keeps_every_problem_and_caps_healthy_in_order() {
    // odd numbers are "problems"; cap healthy (even) at 2.
    let items: Vec<u32> = (1..=10).collect();
    let (kept, omitted) = doctor_bound_healthy_listing(items, 2, |n| !n.is_multiple_of(2));
    assert_eq!(kept, vec![1, 2, 3, 4, 5, 7, 9]);
    assert_eq!(omitted, 3);
}

#[test]
pub(crate) fn small_sets_are_untouched_and_zero_cap_keeps_only_problems() {
    let (kept, omitted) = doctor_bound_healthy_listing(vec![2u32, 4], 64, |n| !n.is_multiple_of(2));
    assert_eq!(kept, vec![2, 4]);
    assert_eq!(omitted, 0);
    let (kept, omitted) =
        doctor_bound_healthy_listing(vec![1u32, 2, 4], 0, |n| !n.is_multiple_of(2));
    assert_eq!(kept, vec![1]);
    assert_eq!(omitted, 2);
}
