import PropTypes from "prop-types";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import { OutlinedInput, Box } from "@mui/material";
import DashboardContext from "../Context";
import { useContext } from "react";
export default function DatasetSelect() {
  // const theme = useTheme();
  const { selectedMetrics, displayMetric, setDisplayMetric } =
    useContext(DashboardContext);

  const handleChange = (event) => {
    setDisplayMetric(event.target.value);
  };

  return (
    <div>
      <FormControl sx={{ m: 1, display: "flex" }}>
        <InputLabel id="demo-multiple-chip-label">
          Select Display Metric
        </InputLabel>
        <Select
          labelId="demo-multiple-chip-label"
          id="demo-multiple-chip"
          value={displayMetric}
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
          {selectedMetrics.map((name) => (
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
