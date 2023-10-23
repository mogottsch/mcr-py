use mlc::bag;

const NEXT_BIKE_PRICE_INCREMENT_INTERVAL_TARIFF: u64 = 30; // minutes
const NEXT_BIKE_PRICE_INCREMENT_INTERVAL_NO_TARIFF: u64 = 15; // minutes
const NEXT_BIKE_PRICE_PER_INCREMENT_INTERVAL: u64 = 100; // cents

const PERSONAL_CAR_PRICE_INCREMENT_INTERVAL: u64 = 1; // minutes
const PERSONAL_CAR_PRICE_PER_INCREMENT_INTERVAL: u64 = 19; // cents

pub fn next_bike_tariff(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
) -> bag::Label<usize> {
    let old_hidden_values = old_label.hidden_values.clone();
    let old_bicycle_duration = old_hidden_values[0];
    let old_bicycle_duration_minutes = old_bicycle_duration / 60;

    let old_price_increment_intervals =
        old_bicycle_duration_minutes / NEXT_BIKE_PRICE_INCREMENT_INTERVAL_TARIFF;

    let new_hidden_values = new_label.hidden_values.clone();
    let new_bicycle_duration = new_hidden_values[0];
    let new_bicycle_duration_minutes = new_bicycle_duration / 60;

    let new_price_increment_intervals =
        new_bicycle_duration_minutes / NEXT_BIKE_PRICE_INCREMENT_INTERVAL_TARIFF;

    let price_increment = new_price_increment_intervals - old_price_increment_intervals;
    let new_price = new_label.values[1] + price_increment * NEXT_BIKE_PRICE_PER_INCREMENT_INTERVAL;
    let new_values = vec![new_label.values[0], new_price];
    bag::Label {
        node_id: new_label.node_id,
        path: new_label.path.clone(),
        values: new_values,
        hidden_values: new_label.hidden_values.clone(),
    }
}

pub fn next_bike_without_tariff(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
) -> bag::Label<usize> {
    let old_hidden_values = old_label.hidden_values.clone();
    let old_bicycle_duration = old_hidden_values[0];
    let old_bicycle_duration_minutes = old_bicycle_duration / 60;

    let old_price_increment_intervals =
        old_bicycle_duration_minutes / NEXT_BIKE_PRICE_INCREMENT_INTERVAL_NO_TARIFF;

    let new_hidden_values = new_label.hidden_values.clone();
    let new_bicycle_duration = new_hidden_values[0];
    let new_bicycle_duration_minutes = new_bicycle_duration / 60;

    let new_price_increment_intervals =
        new_bicycle_duration_minutes / NEXT_BIKE_PRICE_INCREMENT_INTERVAL_NO_TARIFF;

    let mut price_increment = new_price_increment_intervals - old_price_increment_intervals;
    if old_bicycle_duration == 0 && new_bicycle_duration != 0 {
        price_increment = new_price_increment_intervals + 1;
    }
    let new_price = new_label.values[1] + price_increment * NEXT_BIKE_PRICE_PER_INCREMENT_INTERVAL;
    let new_values = vec![new_label.values[0], new_price];
    bag::Label {
        node_id: new_label.node_id,
        path: new_label.path.clone(),
        values: new_values,
        hidden_values: new_label.hidden_values.clone(),
    }
}

pub fn personal_car(
    old_label: &bag::Label<usize>,
    new_label: &bag::Label<usize>,
) -> bag::Label<usize> {
    let old_hidden_values = old_label.hidden_values.clone();
    let old_bicycle_duration = old_hidden_values[0];
    let old_bicycle_duration_minutes = old_bicycle_duration / 60;

    let old_price_increment_intervals =
        old_bicycle_duration_minutes / PERSONAL_CAR_PRICE_INCREMENT_INTERVAL;

    let new_hidden_values = new_label.hidden_values.clone();
    let new_bicycle_duration = new_hidden_values[0];
    let new_bicycle_duration_minutes = new_bicycle_duration / 60;

    let new_price_increment_intervals =
        new_bicycle_duration_minutes / PERSONAL_CAR_PRICE_INCREMENT_INTERVAL;

    let mut price_increment = new_price_increment_intervals - old_price_increment_intervals;
    if old_bicycle_duration == 0 && new_bicycle_duration != 0 {
        price_increment = new_price_increment_intervals + 1;
    }
    let new_price =
        new_label.values[1] + price_increment * PERSONAL_CAR_PRICE_PER_INCREMENT_INTERVAL;
    let new_values = vec![new_label.values[0], new_price];
    bag::Label {
        node_id: new_label.node_id,
        path: new_label.path.clone(),
        values: new_values,
        hidden_values: new_label.hidden_values.clone(),
    }
}
