from teehr.models.loading.nwm30_grid import GridConfigurationModel
from teehr.models.loading.nwm30_point import PointConfigurationModel


def test_point_model():
    configuration = "short_range_alaska"
    output_type = "channel_rt"
    variable_name = "streamflow"

    # Assemble input parameters
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }

    cm = PointConfigurationModel.model_validate(vars)

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    var_name = getattr(forecast_obj, out_type).name

    assert config == "short_range_alaska"
    assert output_type == "channel_rt"
    assert var_name == "streamflow"


def test_grid_model():
    configuration = "forcing_medium_range_blend_alaska"
    output_type = "forcing"
    variable_name = "RAINRATE"

    # Assemble input parameters
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }

    # Check input parameters
    cm = GridConfigurationModel.model_validate(vars)

    config = cm.configuration.name
    forecast_obj = getattr(cm, config)
    out_type = forecast_obj.output_type.name
    var_name = getattr(forecast_obj, out_type).name

    assert config == "forcing_medium_range_blend_alaska"
    assert output_type == "forcing"
    assert var_name == "RAINRATE"


if __name__ == "__main__":
    test_point_model()
    test_grid_model()
    pass
