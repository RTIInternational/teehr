import { Controller } from "react-hook-form";
import { TextField, FormControl } from "@mui/material";
import PropTypes from "prop-types";

const FormInputTextPropTypes = {
  name: PropTypes.string,
  control: PropTypes.any,
  label: PropTypes.string,
  style: PropTypes.object,
  rules: PropTypes.object,
  onChange: PropTypes.func,
};

const FormInputText = ({
  name,
  control,
  label,
  style,
  rules,
  onChange,
  ...props
}) => {
  return (
    <FormControl size={"small"} sx={{ m: 0.5, width: "100%" }}>
      <Controller
        name={name}
        control={control}
        render={({
          field: { onChange: defaultOnChange, value },
          fieldState: { error },
        }) => {
          const handleChange = (event) => {
            if (typeof onChange === "function") {
              onChange(event.target.value, defaultOnChange);
            } else {
              defaultOnChange(event);
            }
          };
          return (
            <TextField
              helperText={error ? error.message : null}
              size="small"
              error={!!error}
              onChange={handleChange}
              value={value}
              fullWidth
              label={label}
              variant="outlined"
              sx={{ ...style }}
              {...props}
            />
          );
        }}
        rules={rules}
      />
    </FormControl>
  );
};

FormInputText.propTypes = FormInputTextPropTypes;

export default FormInputText;
