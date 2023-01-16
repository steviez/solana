use {
    chrono::{serde::ts_nanoseconds, DateTime, Utc},
    clap::{crate_description, crate_name, value_t_or_exit, values_t_or_exit, App, Arg},
    log::*,
    regex::Regex,
    serde::{ser::SerializeMap, Serialize, Serializer},
    std::{
        collections::{hash_map::Entry, HashMap},
        fs::File,
        io::{BufRead, BufReader},
        path::PathBuf,
    },
};

#[derive(Serialize, Debug)]
struct DatapointInstance {
    // The entire datapoint string of the following form
    // datapoint: datapoint_name key1=val1 key2=val2 ...
    #[serde(rename = "fields")]
    #[serde(with = "datapoint_field_serialize")]
    datapoint: String,
    // The timestamp for when this datapoint occurred
    #[serde(rename = "timestamp_ns")]
    #[serde(with = "ts_nanoseconds")]
    timestamp: DateTime<Utc>,
}

mod datapoint_field_serialize {
    use super::*;

    pub(super) fn serialize<S>(datapoint: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        for field in datapoint.split(' ').skip(1) {
            let key_and_val: Vec<_> = field.split('=').collect();
            assert!(key_and_val.len() == 2);
            let key = key_and_val[0];
            let val = key_and_val[1];
            if let Ok(val) = val.parse::<bool>() {
                map.serialize_entry(key, &val)?;
            // Integers will have a trailing 'i' in the datapoint string
            // TODO: this could probably be handled more robustly
            } else if let Ok(val) = val[..val.len() - 1].parse::<i64>() {
                map.serialize_entry(key, &val)?;
            } else if let Ok(val) = val.parse::<f64>() {
                map.serialize_entry(key, &val)?;
            } else {
                // If all else fails, just write the string out as-is
                map.serialize_entry(key, val)?;
            }
        }
        map.end()
    }
}

fn main() {
    solana_logger::setup_with_default("solana=info");
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        //.setting(AppSettings::InferSubcommands)
        //.setting(AppSettings::SubcommandRequiredElseHelp)
        //.setting(AppSettings::VersionlessSubcommands)
        .version(solana_version::version!())
        .arg(
            Arg::with_name("logfile")
                .long("logfile")
                .value_name("PATH")
                .takes_value(true)
                .required(true)
                .help("Path to log file to parse"),
        )
        .arg(
            Arg::with_name("datapoints")
                .long("datapoints")
                .value_name("DATAPOINT_NAMES")
                .takes_value(true)
                .required(true)
                .multiple(true)
                .help("Datapoint name(s) to parse"),
        )
        .get_matches();

    info!("{} {}", crate_name!(), solana_version::version!());

    let datapoints_to_match = values_t_or_exit!(matches, "datapoints", String);
    let path = PathBuf::from(value_t_or_exit!(matches, "logfile", String));
    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);

    let datapoint_regex = Regex::new(
        r"(?x)
        \[
        (?P<timestamp>[^\s]+)
        \s
        (?P<level>[A-Z]+)
        \s\s
        (?P<component>[a-z:_]+)
        \]\sdatapoint:\s
        (?P<datapoint>.*)
    ",
    )
    .unwrap();

    // A map from a list of datapoint names to all parsed occurrences
    let mut found_datapoints: HashMap<String, Vec<DatapointInstance>> = HashMap::new();

    for (line_num, log_line) in reader.lines().enumerate() {
        // Print an udpate every 1M slots so we know we're not locked up
        if line_num % 1000000 == 0 {
            info!("Reading line {}", line_num);
        }

        let log_line = log_line.expect("Read next log line from file");
        let captures = datapoint_regex.captures(&log_line);
        if captures.is_none() {
            // The regex didn't match, so this line is not a datapoint
            continue;
        }
        let captures = captures.unwrap();

        let datapoint = captures
            .name("datapoint")
            .map(|m| String::from(m.as_str()))
            .unwrap();
        // The datapoint's name and all key=value pairs are space separated;
        // the datapoint's name appears first.
        let (name, fields) = datapoint.split_once(' ').unwrap();
        // The datapoint must have some fields and thus, the unparsed
        // fields string must be non-empty.
        if fields.is_empty() {
            continue;
        }
        // Check to make sure this is one of the datapoints of interest
        let datapoint_name = String::from(name);
        if !datapoints_to_match.contains(&datapoint_name) {
            continue;
        }

        let timestamp = captures
            .name("timestamp")
            .map(|m| m.as_str())
            .and_then(|m| m.parse::<DateTime<Utc>>().ok())
            .unwrap();
        let level = captures.name("level").map(|m| m.as_str()).unwrap();
        let component = captures.name("component").map(|m| m.as_str()).unwrap();

        debug!(
            "Found a match for {}\n\
            timestamp: {}\n\
            level: {}\n\
            component: {}\n\
            datapoint: {}",
            datapoint_name, timestamp, level, component, datapoint
        );

        let dp_instance = DatapointInstance {
            datapoint,
            timestamp,
        };

        match found_datapoints.entry(datapoint_name) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(dp_instance);
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![dp_instance]);
            }
        }
    }

    for datapoint in datapoints_to_match.iter() {
        let num_instances = found_datapoints
            .get(datapoint)
            .and_then(|v| Some(v.len()))
            .unwrap_or_default();
        info!(
            "Found {} instance(s) of {} datapoint",
            num_instances, datapoint
        );
    }

    // TOOD: support more types with solana_cli_output::OutputFormat ?
    println!(
        "{}",
        serde_json::to_string_pretty(&found_datapoints).unwrap()
    );
}

#[cfg(test)]
pub mod test {}
