def write_to_sink(processed_df, sink_file_format, sink_output_mode, output_processed_folder_name, output_base_dir):
    (processed_df
     .write
     .format(sink_file_format)
     .mode(sink_output_mode)
     .save(f'{output_base_dir}/{output_processed_folder_name}'))