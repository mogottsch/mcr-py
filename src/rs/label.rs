use mlc::bag;

struct PriceIncrementInfo {
    interval: u64,
    price_per_interval: u64,
    first_interval_free: bool,
}

fn calculate_price_increment_intervals(duration_minutes: u64, info: &PriceIncrementInfo) -> u64 {
    duration_minutes / info.interval
}

fn calculate_new_price(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
    info: &PriceIncrementInfo,
) -> bag::Label<usize> {
    let old_duration_minutes = old_label.hidden_values[0] / 60;
    let new_duration_minutes = new_label.hidden_values[0] / 60;

    let old_price_increment_intervals =
        calculate_price_increment_intervals(old_duration_minutes, info);
    let new_price_increment_intervals =
        calculate_price_increment_intervals(new_duration_minutes, info);

    let mut price_increment = new_price_increment_intervals - old_price_increment_intervals;

    // Handle the case where the old duration is zero, and the new duration is not
    if old_label.hidden_values[0] == 0
        && new_label.hidden_values[0] != 0
        && !info.first_interval_free
    {
        price_increment = new_price_increment_intervals + 1;
    }

    let new_price = new_label.values[1] + price_increment * info.price_per_interval;
    let new_values = vec![new_label.values[0], new_price];

    bag::Label {
        node_id: new_label.node_id,
        path: new_label.path.clone(),
        values: new_values,
        hidden_values: new_label.hidden_values.clone(),
    }
}

const NEXT_BIKE_TARIFF_INFO: PriceIncrementInfo = PriceIncrementInfo {
    interval: 30,
    price_per_interval: 100,
    first_interval_free: true,
};

const NEXT_BIKE_NO_TARIFF_INFO: PriceIncrementInfo = PriceIncrementInfo {
    interval: 15,
    price_per_interval: 100,
    first_interval_free: false,
};

const PERSONAL_CAR_INFO: PriceIncrementInfo = PriceIncrementInfo {
    interval: 1,
    price_per_interval: 19,
    first_interval_free: false,
};

pub fn next_bike_tariff(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
) -> bag::Label<usize> {
    calculate_new_price(old_label, new_label, &NEXT_BIKE_TARIFF_INFO)
}

pub fn next_bike_without_tariff(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
) -> bag::Label<usize> {
    calculate_new_price(old_label, new_label, &NEXT_BIKE_NO_TARIFF_INFO)
}

pub fn personal_car(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
) -> bag::Label<usize> {
    calculate_new_price(old_label, new_label, &PERSONAL_CAR_INFO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestCase {
        old_hidden_values: Vec<u64>,
        new_hidden_values: Vec<u64>,
        expected_price: u64,
        pricing_function: fn(&bag::Label<usize>, &bag::Label<usize>) -> bag::Label<usize>,
    }

    #[test]
    fn test_price_calculations() {
        let test_cases = vec![
            TestCase {
                old_hidden_values: vec![0],
                new_hidden_values: vec![1 * 60],
                expected_price: 0,
                pricing_function: next_bike_tariff,
            },
            TestCase {
                old_hidden_values: vec![1 * 60],
                new_hidden_values: vec![2 * 60],
                expected_price: 0,
                pricing_function: next_bike_tariff,
            },
            TestCase {
                old_hidden_values: vec![1 * 60],
                new_hidden_values: vec![35 * 60],
                expected_price: 100,
                pricing_function: next_bike_tariff,
            },
            TestCase {
                old_hidden_values: vec![25 * 60],
                new_hidden_values: vec![35 * 60],
                expected_price: 100,
                pricing_function: next_bike_tariff,
            },
            TestCase {
                old_hidden_values: vec![0 * 60],
                new_hidden_values: vec![1 * 60],
                expected_price: 100,
                pricing_function: next_bike_without_tariff,
            },
            TestCase {
                old_hidden_values: vec![1 * 60],
                new_hidden_values: vec![2 * 60],
                expected_price: 0,
                pricing_function: next_bike_without_tariff,
            },
            TestCase {
                old_hidden_values: vec![1 * 60],
                new_hidden_values: vec![35 * 60],
                expected_price: 200,
                pricing_function: next_bike_without_tariff,
            },
            TestCase {
                old_hidden_values: vec![25 * 60],
                new_hidden_values: vec![35 * 60],
                expected_price: 100,
                pricing_function: next_bike_without_tariff,
            },
        ];

        for (i, case) in test_cases.iter().enumerate() {
            let old_label = bag::Label {
                hidden_values: case.old_hidden_values.clone(),
                values: vec![0, 0],
                node_id: 0,
                path: vec![],
            };

            let new_label = bag::Label {
                hidden_values: case.new_hidden_values.clone(),
                values: vec![0, 0],
                node_id: 0,
                path: vec![],
            };

            let result_label = (case.pricing_function)(&old_label, &new_label);
            assert_eq!(
                result_label.values[1],
                case.expected_price.clone(),
                "TC[{}] failed with old_label.hidden_values: {:?}, new_label.hidden_values: {:?} \n {:?}",
                i,
                old_label.hidden_values,
                new_label.hidden_values,
                case
            );
        }
    }
}
