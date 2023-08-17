from teehr.loading.nwm22.grid_config_models import GridConfigurationModel
from teehr.loading.nwm22.point_config_models import PointConfigurationModel


def test_point_model():
    configuration = "short_range"
    output_type = "channel_rt"
    variable_name = "streamflow"

    vars = {
        "configuration": configuration,
        "output_type": output_type,
        "variable_name": variable_name,
        configuration: {
            output_type: variable_name,
        },
    }

    cm = PointConfigurationModel.parse_obj(vars)

    assert cm.configuration.name == "short_range"
    assert cm.output_type.name == "channel_rt"
    assert cm.variable_name.name == "streamflow"


def test_grid_model():
    configuration = "short_range"
    output_type = "forcing"
    variable_name = "RAINRATE"

    vars = {
        "configuration": configuration,
        "output_type": output_type,
        "variable_name": variable_name,
        configuration: {
            output_type: variable_name,
        },
    }

    cm = GridConfigurationModel.parse_obj(vars)

    assert cm.configuration.name == "short_range"
    assert cm.output_type.name == "forcing"
    assert cm.variable_name.name == "RAINRATE"


if __name__ == "__main__":
    test_point_model()
    test_grid_model()
    pass
