import PropTypes from "prop-types";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import FormHelperText from "@mui/material/FormHelperText";
import { OutlinedInput, Box } from "@mui/material";

export default function SingleSelect(props) {
  const { options, value, onChange, label, error, errorMessage } = props;

  return (
    <div>
      <FormControl sx={{ display: "flex" }}>
        <InputLabel id="demo-multiple-chip-label">{label}</InputLabel>
        <Select
          labelId="demo-multiple-chip-label"
          id="demo-multiple-chip"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          input={
            <OutlinedInput
              id="select-multiple-chip"
              label="Select Display Metrics"
            />
          }
          renderValue={(selected) => (
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
              {selected}
            </Box>
          )}
          error={error}
          // MenuProps={MenuProps}
        >
          {options.map((name) => (
            <MenuItem
              key={name}
              value={name}
              // style={getStyles(name, selectedDataset, theme)}
            >
              {name}
            </MenuItem>
          ))}
        </Select>
        <FormHelperText>{errorMessage}</FormHelperText>
      </FormControl>
    </div>
  );
}

SingleSelect.propTypes = {
  options: PropTypes.array.isRequired,
  value: PropTypes.string,
  onChange: PropTypes.func.isRequired,
  label: PropTypes.string,
  error: PropTypes.bool,
  errorMessage: PropTypes.string,
};
