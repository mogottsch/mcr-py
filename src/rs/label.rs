use mlc::bag;

struct PriceIncrementInfo {
    interval: u64,
    price_per_interval: u64,
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
    if old_label.hidden_values[0] == 0 && new_label.hidden_values[0] != 0 {
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
};

const NEXT_BIKE_NO_TARIFF_INFO: PriceIncrementInfo = PriceIncrementInfo {
    interval: 15,
    price_per_interval: 100,
};

const PERSONAL_CAR_INFO: PriceIncrementInfo = PriceIncrementInfo {
    interval: 1,
    price_per_interval: 19,
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
