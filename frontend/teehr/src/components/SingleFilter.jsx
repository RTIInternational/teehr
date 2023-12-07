import { useState, useContext } from "react";
import PropTypes from "prop-types";
import SingleSelect from "./SingleSelect";
import OperatorSelect from "./OperatorSelect";
import TextInput from "./TextInput";
import DashboardContext from "../Context";
import { LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterLuxon } from "@mui/x-date-pickers/AdapterLuxon";
import { TimePicker } from "@mui/x-date-pickers/TimePicker";
import { DateTime } from "luxon";
import { Grid, Button } from "@mui/material";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import axios from "axios";
export default function Filter(props) {
  const { selectedGroupByField, selectedOperator, value, index } = props;
  const {
    groupByFields,
    selectedDataset,
    setLoading,
    setErrors,
    setFieldValues,
    updateFilter,
    deleteFilter,
    nonListFields,
  } = useContext(DashboardContext);

  const [fieldType, setFieldType] = useState();
  const [fieldValueOptions, setFieldValueOptions] = useState([]);

  const getFieldType = (fieldName) => {
    const field = groupByFields.find((field) => field.name === fieldName);
    return field ? field.type : null;
  };

  const castFieldValue = (value) => {
    switch (fieldType) {
      case "FLOAT":
        return parseFloat(value);
      case "INTERVAL":
        return parseInt(value);
      case "BOOLEAN":
        return Boolean(value);
      default:
        return value;
    }
  };

  const fetchFieldValues = (selectedGroupByField) => {
    setLoading(true);
    axios
      .post(
        `http://localhost:8000/datasets/${selectedDataset}/get_unique_field_values`,
        {
          field_name: selectedGroupByField,
        }
      )
      .then((res) => {
        const options = res.data.map((o) => Object.values(o)[0]);
        setFieldValueOptions(options);
        setLoading(false);
        setFieldValues((prev) => {
          const newFieldValues = { ...prev };
          newFieldValues[selectedGroupByField] = options;
          return newFieldValues;
        });
      })
      .catch(function (err) {
        setErrors(err);
        setLoading(false);
      });
  };

  const handleFilterFieldChange = (newField) => {
    updateFilter(index, "column", newField);
    updateFilter(index, "value", "");
    const type = getFieldType(newField);
    setFieldType(type);
    if (!nonListFields.includes(newField)) {
      fetchFieldValues(newField);
    }
  };

  const handleOperatorChange = (value) => {
    updateFilter(index, "operator", value);
  };

  const handleFieldValueChange = (value) => {
    updateFilter(index, "value", castFieldValue(value));
  };

  const handleTimeValueChange = (value) => {
    if (value) {
      updateFilter(index, "value", value.toUTC().toISO());
    }
  };

  return (
    <Grid container spacing={1} sx={{ mb: 1 }}>
      <Grid item xs={12} md={4.5}>
        <SingleSelect
          value={selectedGroupByField || ""}
          onChange={handleFilterFieldChange}
          options={groupByFields.map((o) => o.name)}
          label={"Filter"}
        />
      </Grid>
      <Grid item xs={12} md={2}>
        <OperatorSelect
          value={selectedOperator || ""}
          onChange={handleOperatorChange}
        />
      </Grid>
      <Grid item xs={12} md={4.5}>
        {fieldType === "TIMESTAMP" && (
          <LocalizationProvider dateAdapter={AdapterLuxon}>
            <TimePicker
              label="Time"
              value={value ? DateTime.fromISO(value) : null}
              onChange={handleTimeValueChange}
            />
          </LocalizationProvider>
        )}
        {fieldType !== "TIMESTAMP" &&
          !nonListFields.includes(selectedGroupByField) && (
            <SingleSelect
              value={value || ""}
              onChange={handleFieldValueChange}
              options={fieldValueOptions}
              label={"Input"}
            />
          )}
        {fieldType !== "TIMESTAMP" &&
          nonListFields.includes(selectedGroupByField) && (
            <TextInput
              label={"Input"}
              value={value || ""}
              onChange={handleFieldValueChange}
            />
          )}
      </Grid>
      <Grid
        item
        xs={12}
        md={1}
        sx={{
          display: "flex",
          alignContent: "center",
          justifyContent: "center",
        }}
      >
        <Button
          variant="standard"
          onClick={() => deleteFilter(index)}
          color="grey"
        >
          <DeleteOutlineOutlinedIcon />
        </Button>
      </Grid>
    </Grid>
  );
}

Filter.propTypes = {
  index: PropTypes.number,
  value: PropTypes.string,
  selectedGroupByField: PropTypes.string,
  selectedOperator: PropTypes.string,
};
