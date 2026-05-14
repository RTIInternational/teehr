"""Classes representing UDFs."""
from typing import List, Union
from pydantic import Field
import pyspark.sql as ps
from teehr.calculated_fields.models.base import CalculatedFieldABC, CalculatedFieldBaseModel
from teehr.calculated_fields.timeseries_aware_pandas import (
    apply_baseflow_separation_pandas,
    apply_baseflow_period_detection_pandas,
    apply_exceedance_probability_pandas,
    apply_percentile_event_detection_pandas,
    apply_threshold_event_detection_pandas,
)

UNIQUENESS_FIELDS = [
    'reference_time',
    'primary_location_id',
    'configuration_name',
    'variable_name',
    'unit_name'
]


class AbovePercentileEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds "event" and "event_id" columns to the DataFrame based on a percentile threshold.

    The "event" column (bool) indicates whether the value is above the XXth
    percentile. For the "event" column, True values indicate that the
    corresponding value exceeds the specified percentile threshold, while
    False values indicate that the value is below or equal to the threshold.
    The "event_id" column (string) groups continuous segments of events and
    assigns a unique ID to each segment in the format "startdate-enddate".

    Properties
    ----------
    - quantile:
        The percentile threshold to use for event detection.
        Default: 0.85
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to detect events on.
        Default: "primary_value"
    - output_event_field_name:
        The name of the column to store the event detection.
        Default: "event_above"
    - output_event_id_field_name:
        The name of the column to store the event ID.
        Default: "event_above_id"
    - output_quantile_field_name:
        The name of the column to store the quantile value.
        Default: "quantile_value"
    - add_quantile_field:
        Whether to add the quantile field.
        Default: False
    - skip_event_id:
        Whether to skip the event ID generation.
        Default: False
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    quantile: float = Field(
        default=0.85
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_event_field_name: str = Field(
        default="event_above"
    )
    output_event_id_field_name: str = Field(
        default="event_above_id"
    )
    output_quantile_field_name: str = Field(
        default="quantile_value"
    )
    add_quantile_field: bool = Field(
        default=False
    )
    skip_event_id: bool = Field(
        default=False
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_percentile_event_detection_pandas(
            sdf=sdf,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            quantile=self.quantile,
            output_event_field_name=self.output_event_field_name,
            output_event_id_field_name=self.output_event_id_field_name,
            output_quantile_field_name=self.output_quantile_field_name,
            add_quantile_field=self.add_quantile_field,
            skip_event_id=self.skip_event_id,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
            is_above=True,
        )


class BelowPercentileEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds "event" and "event_id" columns to the DataFrame based on a percentile threshold.

    The "event" column (bool) indicates whether the value is below the XXth
    percentile. For the "event" column, True values indicate that the
    corresponding value is below the specified percentile threshold, while
    False values indicate that the value is above or equal to the threshold.
    The "event_id" column (string) groups continuous segments of events and
    assigns a unique ID to each segment in the format "startdate-enddate".

    Properties
    ----------
    - quantile:
        The percentile threshold to use for event detection.
        Default: 0.15
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to detect events on.
        Default: "primary_value"
    - output_event_field_name:
        The name of the column to store the event detection.
        Default: "event_below"
    - output_event_id_field_name:
        The name of the column to store the event ID.
        Default: "event_below_id"
    - output_quantile_field_name:
        The name of the column to store the quantile value.
        Default: "quantile_value"
    - add_quantile_field:
        Whether to add the quantile field.
        Default: False
    - skip_event_id:
        Whether to skip the event ID generation.
        Default: False
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """
    quantile: float = Field(
        default=0.15
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_event_field_name: str = Field(
        default="event_below"
    )
    output_event_id_field_name: str = Field(
        default="event_below_id"
    )
    output_quantile_field_name: str = Field(
        default="quantile_value"
    )
    add_quantile_field: bool = Field(
        default=False
    )
    skip_event_id: bool = Field(
        default=False
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_percentile_event_detection_pandas(
            sdf=sdf,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            quantile=self.quantile,
            output_event_field_name=self.output_event_field_name,
            output_event_id_field_name=self.output_event_id_field_name,
            output_quantile_field_name=self.output_quantile_field_name,
            add_quantile_field=self.add_quantile_field,
            skip_event_id=self.skip_event_id,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
            is_above=False,
        )


class AboveThresholdEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds "event" and "event_id" columns to the DataFrame based on a threshold field.

    The "event" column (bool) indicates whether the value is above the threshold
    read from a specified column. True values indicate that the corresponding
    value exceeds the threshold, while False values indicate that the value is
    below or equal to the threshold. Threshold values are cast to float before
    comparison to handle attribute fields, which are always stored as strings.
    The "event_id" column (string) groups continuous segments of events and
    assigns a unique ID to each segment in the format "startdate-enddate".

    Properties
    ----------
    - threshold_field_name:
        The name of the column containing the threshold value. Threshold
        values are cast to float before use, so string attribute fields
        are supported.
        Default: "threshold"
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to detect events on.
        Default: "primary_value"
    - output_event_field_name:
        The name of the column to store the event detection.
        Default: "event_above"
    - output_event_id_field_name:
        The name of the column to store the event ID.
        Default: "event_above_id"
    - skip_event_id:
        Whether to skip the event ID generation.
        Default: False
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    threshold_field_name: str = Field(
        default="threshold"
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_event_field_name: str = Field(
        default="event_above"
    )
    output_event_id_field_name: str = Field(
        default="event_above_id"
    )
    skip_event_id: bool = Field(
        default=False
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_threshold_event_detection_pandas(
            sdf=sdf,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            threshold_field_name=self.threshold_field_name,
            output_event_field_name=self.output_event_field_name,
            output_event_id_field_name=self.output_event_id_field_name,
            skip_event_id=self.skip_event_id,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
            is_above=True,
        )


class BelowThresholdEventDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Adds "event" and "event_id" columns to the DataFrame based on a threshold field.

    The "event" column (bool) indicates whether the value is below the threshold
    read from a specified column. True values indicate that the corresponding
    value is below the threshold, while False values indicate that the value is
    above or equal to the threshold. Threshold values are cast to float before
    comparison to handle attribute fields, which are always stored as strings.
    The "event_id" column (string) groups continuous segments of events and
    assigns a unique ID to each segment in the format "startdate-enddate".

    Properties
    ----------
    - threshold_field_name:
        The name of the column containing the threshold value. Threshold
        values are cast to float before use, so string attribute fields
        are supported.
        Default: "threshold"
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to detect events on.
        Default: "primary_value"
    - output_event_field_name:
        The name of the column to store the event detection.
        Default: "event_below"
    - output_event_id_field_name:
        The name of the column to store the event ID.
        Default: "event_below_id"
    - skip_event_id:
        Whether to skip the event ID generation.
        Default: False
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    threshold_field_name: str = Field(
        default="threshold"
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_event_field_name: str = Field(
        default="event_below"
    )
    output_event_id_field_name: str = Field(
        default="event_below_id"
    )
    skip_event_id: bool = Field(
        default=False
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_threshold_event_detection_pandas(
            sdf=sdf,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            threshold_field_name=self.threshold_field_name,
            output_event_field_name=self.output_event_field_name,
            output_event_id_field_name=self.output_event_id_field_name,
            skip_event_id=self.skip_event_id,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
            is_above=False,
        )


class ExceedanceProbability(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Calculates exceedance probability for a flow duration curve.

    This class computes exceedance probability statistics for a given
    timeseries of streamflow data. It adds the column 'exceedance_probability'
    to the DataFrame, representing the probability of exceedance for each flow
    value. The returned column is returned as a float between 0 and 1 unless
    otherwise specified using the as_percentile property (False by default).

    Properties
    ----------
    - as_percentile: bool
        Whether to return the exceedance probability as a percentile (0-100)
        or a probability (0-1). Returns probability by default.
        Default: False
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the streamflow values.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the exceedance probability information.
        Default: "exceedance_probability"
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]

    """

    as_percentile: bool = Field(
        default=False
    )
    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="exceedance_probability"
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_exceedance_probability_pandas(
            sdf=sdf,
            as_percentile=self.as_percentile,
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class BaseflowPeriodDetection(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Determines baseflow dominated periods.

    This class identifies periods where baseflow is dominant in the streamflow
    timeseries by adding two columns. The 'baseflow_period' column (bool)
    indicates whether the baseflow portion of the streamflow timeseries exceeds
    the quickflow portion, and the 'baseflow_period_id' column (string) groups
    continuous segments of baseflow dominated periods and assigns a unique ID
    to each segment in the format "startdate-enddate". Users can define a
    custom 'event_threshold' value to adjust the sensitivity of baseflow
    detection by applying a multiplier to the quickflow portion of the
    streamflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to compare with baseflow.
        Default: "primary_value"
    - baseflow_field_name:
        The name of the column containing the baseflow values.
        Default: None
    - event_threshold:
        The threshold multiplier value to determine event periods. The
        multiplier is applied to the quickflow portion of the streamflow
        timeseries when determining if the streamflow timeseries is
        dominated by baseflow.
        Default: 1.0
    - output_baseflow_period_field_name:
        The name of the column to store the baseflow period information.
        Default: "baseflow_period"
    - output_baseflow_period_id_field_name:
        The name of the column to store the baseflow period ID information.
        Default: "baseflow_period_id"
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    baseflow_field_name: str = Field(
        default=None
    )
    event_threshold: float = Field(
        default=1.0
    )
    output_baseflow_period_field_name: str = Field(
        default="baseflow_period"
    )
    output_baseflow_period_id_field_name: str = Field(
        default="baseflow_period_id"
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_period_detection_pandas(
            sdf=sdf,
            value_time_field_name=self.value_time_field_name,
            value_field_name=self.value_field_name,
            baseflow_field_name=self.baseflow_field_name,
            event_threshold=self.event_threshold,
            output_baseflow_period_field_name=self.output_baseflow_period_field_name,
            output_baseflow_period_id_field_name=self.output_baseflow_period_id_field_name,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class LyneHollickBaseflow(CalculatedFieldABC,
                          CalculatedFieldBaseModel):
    """Baseflow separation using the Lyne-Hollick method.

    This class implements the Lyne-Hollick digital filter method, which
    separates baseflow from quickflow using a timeseries of streamflow data.
    Adds a column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "lyne_hollick_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="lyne_hollick_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="lyne_hollick",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params=None,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class ChapmanBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Chapman method.

    This class implements the Chapman filter method, which separates baseflow
    from quickflow using a timeseries of streamflow data. Adds a column to
    the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "chapman_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Chapman filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="chapman_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="chapman",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"a": self.a},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class ChapmanMaxwellBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Chapman-Maxwell method.

    This class implements the Chapman-Maxwell filter method, which separates
    baseflow from quickflow using a timeseries of streamflow data. Adds a
    column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "chapman_maxwell_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Chapman-Maxwell filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="chapman_maxwell_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="chapman_maxwell",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"a": self.a},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class BoughtonBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Boughton method.

    This class implements the Boughton double-parameter filter method,
    which separates baseflow from quickflow using a timeseries of streamflow
    data. Adds a column to the joined timeseries table with the baseflow
    timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "boughton_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Boughton filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - c: float
        The shape parameter for the Boughton filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="boughton_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    c: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="boughton",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"a": self.a, "c": self.c},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class FureyBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Furey method.

    This class implements the Furey digital filter method, which separates
    baseflow from quickflow using a timeseries of streamflow data. Adds a
    column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "furey_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Furey filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - c: float
        The shape parameter for the Furey filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="furey_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    c: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="furey",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"a": self.a, "c": self.c},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class EckhardtBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Eckhardt method.

    This class implements the Eckhardt filter method, which separates baseflow
    from quickflow using a timeseries of streamflow data. Adds a column to
    the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "eckhardt_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Eckhardt filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - BFImax: float
        The maximum baseflow index for the Eckhardt filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="eckhardt_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    BFImax: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="eckhardt",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"a": self.a, "BFImax": self.BFImax},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class EWMABaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the EWMA method.

    This class implements the exponential moving average (EWMA) filter method,
    which separates baseflow from quickflow using a timeseries of streamflow
    data. Adds a column to the joined timeseries table with the baseflow
    timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "ewma_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - e: float
        The smoothing parameter for the EWMA filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="ewma_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    e: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="ewma",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"e": self.e},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class WillemsBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the Willems method.

    This class implements the Willems digital filter method, which separates
    baseflow from quickflow using a timeseries of streamflow data. Adds a
    column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "willems_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - a: float
        The recession coefficient for the Willems filter method. If not
        provided, it will be estimated using the input timeseries data.
        Default: None
    - w: float
        The case-specific average quickflow proportion of the streamflow
        used in the Willems filter method. If not provided, it will be
        estimated using the input timeseries data.
        Default: None
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="willems_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    a: float = Field(
        default=None
    )
    w: float = Field(
        default=None
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="willems",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params={"a": self.a, "w": self.w},
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class UKIHBaseflow(CalculatedFieldABC, CalculatedFieldBaseModel):
    """Baseflow separation using the UKIH method.

    This class implements the United Kingdom Institute of Hydrology (UKIH)
    filter method (also referred to as the smoothed minima method), which
    separates baseflow from quickflow using a timeseries of streamflow data.
    Adds a column to the joined timeseries table with the baseflow timeseries.

    Properties
    ----------
    - value_time_field_name:
        The name of the column containing the timestamp.
        Default: "value_time"
    - value_field_name:
        The name of the column containing the value to separate baseflow from.
        Default: "primary_value"
    - output_field_name:
        The name of the column to store the baseflow separation result.
        Default: "ukih_baseflow"
    - beta:
        The filter parameter for the Lyne-Hollick filter method.
        Default: 0.925
    - uniqueness_fields:
        The columns to use to uniquely identify each timeseries.

        .. code-block:: python

            Default: [
                'reference_time',
                'primary_location_id',
                'configuration_name',
                'variable_name',
                'unit_name'
            ]
    """

    value_time_field_name: str = Field(
        default="value_time"
    )
    value_field_name: str = Field(
        default="primary_value"
    )
    output_field_name: str = Field(
        default="ukih_baseflow"
    )
    beta: float = Field(
        default=0.925
    )
    uniqueness_fields: Union[str, List[str]] = Field(
        default=None
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        return apply_baseflow_separation_pandas(
            sdf=sdf,
            method="ukih",
            output_field_name=self.output_field_name,
            value_field_name=self.value_field_name,
            value_time_field_name=self.value_time_field_name,
            beta=self.beta,
            params=None,
            uniqueness_fields=self.uniqueness_fields,
            default_uniqueness_fields=UNIQUENESS_FIELDS,
        )


class TimeseriesAwareCalculatedFields():
    """Timeseries aware calculated fields.

    Notes
    -----
    Timeseries aware CFs are aware of ordered groups of data (e.g., a timeseries).
    This is useful for things such as event detection, base flow separation, and
    other fields that need to be calculated based on a entire timeseries.  The
    definition of what creates a unique set of timeseries (i.e., a timeseries) can
    be specified.

    Available Calculated Fields:

    - AbovePercentileEventDetection
    - BelowPercentileEventDetection
    - AboveThresholdEventDetection
    - BelowThresholdEventDetection
    - ExceedanceProbability
    - BaseflowPeriodDetection
    - LyneHollickBaseflow
    - ChapmanBaseflow
    - ChapmanMaxwellBaseflow
    - BoughtonBaseflow
    - FureyBaseflow
    - EckhardtBaseflow
    - EWMABaseflow
    - WillemsBaseflow
    - UKIHBaseflow

    Examples
    -------
    Add a timeseries aware calculated field to the joined timeseries table.

    >>> from teehr import TimeseriesAwareCalculatedFields as tcf

    >>> ped = tcf.AbovePercentileEventDetection()
    >>> ev.joined_timeseries.add_calculated_fields(ped).write()
    """

    AbovePercentileEventDetection = AbovePercentileEventDetection
    BelowPercentileEventDetection = BelowPercentileEventDetection
    AboveThresholdEventDetection = AboveThresholdEventDetection
    BelowThresholdEventDetection = BelowThresholdEventDetection
    ExceedanceProbability = ExceedanceProbability
    BaseflowPeriodDetection = BaseflowPeriodDetection
    LyneHollickBaseflow = LyneHollickBaseflow
    ChapmanBaseflow = ChapmanBaseflow
    ChapmanMaxwellBaseflow = ChapmanMaxwellBaseflow
    BoughtonBaseflow = BoughtonBaseflow
    FureyBaseflow = FureyBaseflow
    EckhardtBaseflow = EckhardtBaseflow
    EWMABaseflow = EWMABaseflow
    WillemsBaseflow = WillemsBaseflow
    UKIHBaseflow = UKIHBaseflow
