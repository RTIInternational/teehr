import PropTypes from "prop-types";
import FormControl from "@mui/material/FormControl";
import TextField from "@mui/material/TextField";

export default function TextInput(props) {
  const { value, onChange, label, error, helperText } = props;
  return (
    <div>
      <FormControl sx={{ display: "flex" }}>
        <TextField
          error={error}
          helperText={helperText}
          id="outlined-controlled"
          label={label}
          value={value}
          onChange={(e) => onChange(e.target.value)}
        />
      </FormControl>
    </div>
  );
}

TextInput.propTypes = {
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  label: PropTypes.string,
  error: PropTypes.bool,
  helperText: PropTypes.string,
};
