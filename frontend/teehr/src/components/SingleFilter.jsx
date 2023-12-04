import { useEffect, useState, useContext } from "react";
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
  const { groupByFields, selectedDataset, setLoading, setErrors } =
    useContext(DashboardContext);
  const {
    selectedGroupByField,
    selectedOperator,
    value,
    setSelectedGroupByField,
    setSelectedOperator,
    setValue,
    deleteFilter,
  } = props;

  const [error, setError] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [type, setType] = useState();
  const [valueOptions, setValueOptions] = useState([]);

  const getFieldType = (fieldName) => {
    const field = groupByFields.find((field) => field.name === fieldName);
    return field ? field.type : null;
  };

  const validateTypes = (input, type) => {
    if (type === "FLOAT" && isNaN(Number(input))) {
      setError(true);
      setErrorMessage("Input must be a number.");
      return false;
    } else if (
      type === "TIMESTAMP" &&
      new Date(input).toString() === "Invalid Date"
    ) {
      setError(true);
      setErrorMessage("Invalid date.");
      return false;
    }
    setErrorMessage("");
    setError(false);
  };

  const castType = (value) => {
    switch (type) {
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

  useEffect(() => {
    const validateInput = (value) => {
      if (!value) return;
      const type = getFieldType(selectedGroupByField);
      if (!type) {
        setErrorMessage("Missing group by field");
        setError(true);
      } else if (!selectedOperator) {
        setErrorMessage("Missing operator");
        setError(true);
      } else {
        validateTypes(value, type);
      }
    };
    validateInput(value);
  }, [value, selectedGroupByField, selectedOperator, groupByFields]);

  useEffect(() => {
    if (selectedGroupByField) {
      const type = getFieldType(selectedGroupByField);
      setType(type);
      setValue("");
      if (type !== "TIMESTAMP") {
        setLoading(true);
        axios
          .post(
            `http://localhost:8000/datasets/${selectedDataset}/get_unique_field_values`,
            {
              field_name: selectedGroupByField,
            }
          )
          .then((res) => {
            console.log(res.data);
            setLoading(false);
            console.log({ options: res.data.map((o) => Object.values(o)[0]) });
            setValueOptions(res.data.map((o) => Object.values(o)[0]));
          })
          .catch(function (err) {
            console.log(err);
            setErrors(err);
            setLoading(false);
          });
      }
    }
  }, [selectedGroupByField]);

  return (
    <Grid container spacing={0}>
      <Grid item xs={12} md={4.5}>
        <SingleSelect
          options={groupByFields.map((o) => o.name)}
          selectedOption={selectedGroupByField || ""}
          setSelectedOption={setSelectedGroupByField}
          label={"Group By Field"}
        />
      </Grid>
      <Grid item xs={12} md={2}>
        <OperatorSelect
          selectedOperator={selectedOperator || ""}
          setSelectedOperator={setSelectedOperator}
        />
      </Grid>
      <Grid item xs={12} md={4.5}>
        {type === "TIMESTAMP" && (
          <LocalizationProvider dateAdapter={AdapterLuxon}>
            <TimePicker
              label="Time"
              value={value ? DateTime.fromISO(value) : null}
              onChange={(value) => {
                if (value) {
                  setValue(value.toUTC().toISO());
                } else {
                  null;
                }
              }}
              sx={{ m: "8px" }}
            />
          </LocalizationProvider>
        )}
        {type !== "TIMESTAMP" && (
          <SingleSelect
            options={valueOptions}
            selectedOption={value || ""}
            setSelectedOption={(value) => setValue(castType(value))}
            label={"Input"}
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
        <Button variant="standard" onClick={deleteFilter} color="grey">
          <DeleteOutlineOutlinedIcon />
        </Button>
      </Grid>
    </Grid>
  );
}

Filter.propTypes = {
  selectedGroupByField: PropTypes.string,
  selectedOperator: PropTypes.string,
  value: PropTypes.string,
  setSelectedGroupByField: PropTypes.func,
  setSelectedOperator: PropTypes.func,
  setValue: PropTypes.func,
  deleteFilter: PropTypes.func,
};
