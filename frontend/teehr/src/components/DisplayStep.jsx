import "../App.css";
import {
  Grid,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from "@mui/material";
import DisplayMetricSelect from "./DisplayMetricSelect.jsx";
import DashboardContext from "../Context.js";
import { useContext } from "react";

const DisplayStep = () => {
  const {
    selectedDataset,
    selectedGroupByFields,
    fieldValues,
    inputValues,
    setInputValues,
    nonListFields,
  } = useContext(DashboardContext);
  const handleInputChange = (e, fieldName) => {
    setInputValues({
      ...inputValues,
      [fieldName]: e.target.value,
    });
  };

  return (
    <Grid>
      {selectedDataset && <DisplayMetricSelect />}
      {selectedGroupByFields &&
        selectedGroupByFields
          .filter((f) => f.name !== "primary_location_id")
          .map((field, index) => (
            <div
              key={index}
              style={{
                margin: "8px 4px 0px 8px",
                display: "flex",
              }}
            >
              <TextField value={field.name} disabled sx={{ width: "50%" }} />
              <div
                style={{
                  margin: "0px 4px 0px 8px",
                  width: "50%",
                }}
              >
                {field.name in fieldValues &&
                !nonListFields.includes(field.name) ? (
                  <FormControl fullWidth>
                    <InputLabel id={`${field.name}-select-label`}>
                      Input
                    </InputLabel>
                    <Select
                      labelId={`${field.name}-select-label`}
                      id="demo-simple-select"
                      value={inputValues[field.name] || ""}
                      label="Input"
                      onChange={(e) => handleInputChange(e, field.name)}
                    >
                      {fieldValues[field.name].map((value, index) => (
                        <MenuItem key={index} value={value}>
                          {value}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                ) : (
                  <FormControl fullWidth>
                    <TextField
                      value={inputValues[field.name] || ""}
                      onChange={(e) => handleInputChange(e, field.name)}
                    />
                  </FormControl>
                )}
              </div>
            </div>
          ))}
    </Grid>
  );
};

export default DisplayStep;
