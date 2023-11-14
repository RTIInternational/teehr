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
export default function Filter(props) {
  const { groupByFields } = useContext(DashboardContext);
  const {
    selectedGroupByField,
    selectedOperator,
    value,
    setSelectedGroupByField,
    setSelectedOperator,
    setValue,
  } = props;

  const [error, setError] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [type, setType] = useState();

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
    }
  }, [selectedGroupByField]);

  return (
    <div style={{ marginBottom: "8px", display: "flex" }}>
      <div style={{ flex: 4, minWidth: "200px" }}>
        <SingleSelect
          options={groupByFields.map((o) => o.name)}
          selectedOption={selectedGroupByField || ""}
          setSelectedOption={setSelectedGroupByField}
        />
      </div>
      <div style={{ flex: 2 }}>
        <OperatorSelect
          selectedOperator={selectedOperator || ""}
          setSelectedOperator={setSelectedOperator}
        />
      </div>
      <div style={{ flex: 4 }}>
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
          <TextInput
            label={"Input"}
            value={value || ""}
            setValue={setValue}
            error={error}
            helperText={errorMessage}
          />
        )}
      </div>
    </div>
  );
}

Filter.propTypes = {
  selectedGroupByField: PropTypes.string,
  selectedOperator: PropTypes.string,
  value: PropTypes.string,
  setSelectedGroupByField: PropTypes.func,
  setSelectedOperator: PropTypes.func,
  setValue: PropTypes.func,
};
