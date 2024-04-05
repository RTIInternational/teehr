import { FormHelperText, FormControl } from "@mui/material";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterLuxon } from "@mui/x-date-pickers/AdapterLuxon";
import { Controller } from "react-hook-form";
import { DateTime } from "luxon";
import { TimePicker } from "@mui/x-date-pickers/TimePicker";
import PropTypes from "prop-types";

const FormTimePicker = ({ name, control, label, rules, style, onChange }) => {
  return (
    <FormControl size={"small"} sx={{ width: "100%" }}>
      <LocalizationProvider dateAdapter={AdapterLuxon}>
        <Controller
          name={name}
          control={control}
          render={({
            field: { onChange: defaultOnChange, value },
            fieldState: { error },
          }) => {
            const dateTimeValue = value ? DateTime.fromISO(value) : null;
            const handleDateChange = (date) => {
              if (date) {
                defaultOnChange(date.toUTC().toISO());
              } else {
                defaultOnChange(null);
              }
            };
            const handleChange = (event) => {
              if (typeof onChange === "function") {
                onChange(event, handleDateChange);
              } else {
                handleDateChange(event);
              }
            };
            return (
              <>
                <TimePicker
                  value={dateTimeValue}
                  onChange={handleChange}
                  label={label}
                  sx={{ ...style }}
                  fullWidth
                  slotProps={{ textField: { size: "small" } }}
                />{" "}
                <FormHelperText error={true}>
                  {error ? error.message : ""}
                </FormHelperText>
              </>
            );
          }}
          rules={rules}
        />
      </LocalizationProvider>
    </FormControl>
  );
};

FormTimePicker.propTypes = {
  name: PropTypes.string.isRequired,
  control: PropTypes.any.isRequired,
  label: PropTypes.string.isRequired,
  style: PropTypes.object,
  rules: PropTypes.object,
  onChange: PropTypes.func,
};

export default FormTimePicker;
