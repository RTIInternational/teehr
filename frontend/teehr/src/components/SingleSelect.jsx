import PropTypes from "prop-types";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import { OutlinedInput, Box } from "@mui/material";

export default function SingleSelect(props) {
  const { options, selectedOption, setSelectedOption } = props;

  const handleChange = (event) => {
    setSelectedOption(event.target.value);
  };

  return (
    <div>
      <FormControl sx={{ m: 1, display: "flex" }}>
        <InputLabel id="demo-multiple-chip-label">Group By Field</InputLabel>
        <Select
          labelId="demo-multiple-chip-label"
          id="demo-multiple-chip"
          value={selectedOption}
          onChange={handleChange}
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
      </FormControl>
    </div>
  );
}

SingleSelect.propTypes = {
  options: PropTypes.array.isRequired,
  selectedOption: PropTypes.string,
  setSelectedOption: PropTypes.func.isRequired,
};
