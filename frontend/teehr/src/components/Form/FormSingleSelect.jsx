import {
  FormControl,
  FormHelperText,
  InputLabel,
  MenuItem,
  Select,
} from "@mui/material";
import { Controller } from "react-hook-form";
import PropTypes from "prop-types";

const ITEM_HEIGHT = 48;
const ITEM_PADDING_TOP = 8;
const MenuProps = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
      width: 250,
    },
  },
};

const FormSingleSelect = ({
  name,
  control,
  label,
  options,
  rules,
  onChange,
  formStyle,
}) => {
  return (
    <FormControl size={"small"} sx={{ m: 0.5, width: "100%", ...formStyle }}>
      <InputLabel>{label}</InputLabel>
      <Controller
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
            <>
              <Select
                onChange={handleChange}
                value={value}
                label={label}
                fullWidth
                MenuProps={MenuProps}
              >
                {options.map((option) => (
                  <MenuItem key={option} value={option}>
                    {option}
                  </MenuItem>
                ))}
              </Select>
              <FormHelperText error={true}>
                {error ? error.message : ""}
              </FormHelperText>
            </>
          );
        }}
        control={control}
        name={name}
        rules={rules}
      />
    </FormControl>
  );
};

FormSingleSelect.propTypes = {
  name: PropTypes.string.isRequired,
  control: PropTypes.any.isRequired,
  label: PropTypes.string.isRequired,
  options: PropTypes.array,
  style: PropTypes.object,
  rules: PropTypes.object,
  onChange: PropTypes.func,
  formStyle: PropTypes.object,
};

export default FormSingleSelect;
