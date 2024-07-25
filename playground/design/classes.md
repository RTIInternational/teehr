TeehrBasePath()
    TimeseriesLocalPath()
    TimeseriesS3Path()

    LocationLocalPath()
    LocationS3Path()

    AttributeLocalPath()
    AttributeS3Path()

    CrosswalkLocalPath()
    CrosswalkS3Path()
    ...
    properties:
        dir_path
        pattern
    methods:
        validate_schema()
        fix_schema()
        validate_fields()
        fetch_from_xxx()

TeehrDataset()
    TeehrLocalDataset()
    TeehrS3Dataset()
    ...
    properties:
        ...
    methods:
        get_metrics()
        get_timeseries()
        get_timeseries_chars()
        get_crosswalks()
        get_attributes()
        get_joined_timeseries()

TeehrConfig()
    properties:
        config_filepath: Path()
        dataset: TeehrDataset()
        allowable_values:
            - timeseries_units
                - ft^3/s
                - m^3/s
            - timeseries_variables
                - streamflow_daily_mean
                - streamflow_hourly_inst
            - timeseries_configurations
    methods:
        read()
        write()



