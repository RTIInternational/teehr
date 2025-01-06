import pandas as pd
import hvplot.pandas # noqa: F401


def plot_teehr_timeseries(
    df: pd.DataFrame,
    x_column: str = "value_time",
    y_column: str = "value"
):
    columns = df.columns

    constant_columns = [col for col in columns if df[col].nunique() == 1]
    # print(constant_columns)

    constant_column_values = df[constant_columns].iloc[0].to_dict()
    # print(constant_column_values)

    variable_columns = [col for col in columns if df[col].nunique() > 1]
    variable_columns.remove(x_column)
    variable_columns.remove(y_column)

    # print(variable_columns)

    constants = '\n  '.join([f'{k}={v}' for k, v in constant_column_values.items()])
    print(f"Constant values: \n  {constants}")

    return df.hvplot(x="value_time", y="value", by=variable_columns)