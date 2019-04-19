#ifndef NEMO_INCLUDE_NEMO_OPTIONS_H_
#define NEMO_INCLUDE_NEMO_OPTIONS_H_

namespace nemo {

struct Options {
    bool create_if_missing;
    int write_buffer_size;
    int max_open_files;
    bool use_bloomfilter;
    int write_threads;

    // default target_file_size_base and multiplier is the save as rocksdb
    int target_file_size_base;
    int target_file_size_multiplier;
    bool compression;
    int max_background_flushes;
    int max_background_compactions;
    int max_bytes_for_level_multiplier;

    int max_bytes_for_level_base;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    int min_write_buffer_number_to_merge;
    int level0_file_num_compaction_trigger;
    int delayed_write_rate;
    int max_write_buffer_number;
    bool disable_wal;

	Options() : create_if_missing(true),
        write_buffer_size(64 * 1024 * 1024),
        max_open_files(5000),
        use_bloomfilter(true),
        write_threads(71),
        target_file_size_base(64 * 1024 * 1024),
        target_file_size_multiplier(1),
        compression(true),
        max_background_flushes(1),
        max_background_compactions(1),
        max_bytes_for_level_multiplier(10),

        max_bytes_for_level_base(256 * 1024 *1024),
        level0_slowdown_writes_trigger(20),
        level0_stop_writes_trigger(32),
        min_write_buffer_number_to_merge(1),
        level0_file_num_compaction_trigger(4),
        delayed_write_rate(2 * 1024 * 1024),
        max_write_buffer_number(2),
        disable_wal(false) {}
};

}; // end namespace nemo

#endif
