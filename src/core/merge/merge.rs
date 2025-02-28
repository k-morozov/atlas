use std::path::Path;

use crate::core::{
    disk_table::{
        disk_table::{get_disk_table_name_by_level, get_disk_table_path, ReaderDiskTableIterator},
        id::DiskTableID,
        local::local_disk_table_builder::DiskTableBuilder,
    },
    field::FlexibleField,
};

use crate::core::disk_table::utils::{
    LevelsReaderDiskTables, SEGMENTS_MAX_LEVEL, SEGMENTS_MIN_LEVEL,
};
use crate::core::storage::config;

pub fn is_ready_to_merge(table: &LevelsReaderDiskTables) -> bool {
    table[&SEGMENTS_MIN_LEVEL].len() == config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL
}

// pub fn merge_disk_tables(
//     storages: &mut LevelsReaderDiskTables,
//     stoarge_path: &Path,
//     sgm_id: &mut DiskTableID,
// ) {
//     for merging_level in SEGMENTS_MIN_LEVEL..=SEGMENTS_MAX_LEVEL {
//         let disk_table_id = sgm_id.get_and_next();

//         // @todo
//         match storages.get(&merging_level) {
//             Some(segments_by_level) => {
//                 if segments_by_level.len() != config::DEFAULT_DISK_TABLES_LIMIT_BY_LEVEL {
//                     continue;
//                 }
//             }
//             None => continue,
//         }

//         let level_for_new_sg = if merging_level != SEGMENTS_MAX_LEVEL {
//             merging_level + 1
//         } else {
//             merging_level
//         };

//         let segment_name = get_disk_table_name_by_level(disk_table_id, level_for_new_sg);
//         let disk_table_path = get_disk_table_path(stoarge_path, &segment_name);
//         let merging_segments = &storages[&merging_level];

//         let mut its = merging_segments
//             .iter()
//             .map(|disk_table| disk_table.into_iter())
//             .collect::<Vec<ReaderDiskTableIterator<FlexibleField, FlexibleField>>>();
//         let mut entries = its.iter_mut().map(|it| it.next()).collect::<Vec<_>>();
//         let mut builder = DiskTableBuilder::new(disk_table_path.as_path());

//         while entries.iter().any(|v| v.is_some()) {
//             let (index, entry) = entries
//                 .iter()
//                 .enumerate()
//                 .filter(|(_index, v)| v.is_some())
//                 .map(|(index, entry)| {
//                     let e = entry.as_ref().unwrap();
//                     (index, e)
//                 })
//                 .collect::<Vec<_>>()
//                 // @todo iter vs into_iter
//                 .into_iter()
//                 .min_by(|lhs, rhs| lhs.1.get_key().cmp(rhs.1.get_key()))
//                 .unwrap();

//             builder.append_entry(entry);

//             if let Some(it) = its.get_mut(index) {
//                 entries[index] = it.next();
//             }
//         }

//         let Ok(merged_disk_table) = builder.build() else {
//             panic!("Failed create disk table for merge_disk_tables")
//         };

//         for merging_disk_table in storages.get_mut(&merging_level).unwrap() {
//             match merging_disk_table.remove() {
//                 Ok(_) => {}
//                 Err(er) => panic!(
//                     "failed remove merged disk table: path={}, error={}",
//                     merging_disk_table.get_path().display(),
//                     er,
//                 ),
//             }
//         }

//         storages.get_mut(&merging_level).unwrap().clear();
//         storages
//             .entry(level_for_new_sg)
//             .or_insert_with(Vec::new)
//             .push(merged_disk_table);
//     }
// }
