import {
  Checkbox,
  FormControl,
  FormControlLabel,
  FormHelperText,
} from "@mui/material";
import { Controller } from "react-hook-form";
import PropTypes from "prop-types";

const FormCheckbox = ({ name, control, label, rules, onChange }) => {
  return (
    <FormControl size="small" variant="outlined">
      <FormControlLabel
        control={
          <Controller
            control={control}
            name={name}
            rules={rules}
            render={({
              field: { onChange: defaultOnChange, value },
              fieldState: { error },
            }) => {
              const handleChange = (event) => {
                if (typeof onChange === "function") {
                  onChange(event.target.checked, defaultOnChange);
                } else {
                  defaultOnChange(event.target.checked);
                }
              };
              return (
                <>
                  <Checkbox checked={value} onChange={handleChange} />
                  {error && (
                    <FormHelperText error>{error.message}</FormHelperText>
                  )}
                </>
              );
            }}
          />
        }
        label={label}
      />
    </FormControl>
  );
};

FormCheckbox.propTypes = {
  name: PropTypes.string.isRequired,
  control: PropTypes.any.isRequired,
  label: PropTypes.string.isRequired,
  rules: PropTypes.object,
  onChange: PropTypes.func,
};

export default FormCheckbox;
