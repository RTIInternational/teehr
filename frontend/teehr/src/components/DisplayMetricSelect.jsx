import PropTypes from "prop-types";
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import { OutlinedInput, Box } from '@mui/material';

export default function DatasetSelect(props) {
  // const theme = useTheme();
  const { selectedMetrics, selectedDisplayMetric, setSelectedDisplayMetric } = props;


  const handleChange = (event) => {
    setSelectedDisplayMetric(event.target.value);
  };

  return (
    <div>
      <FormControl sx={{ m: 1, display: 'flex'}}>
        <InputLabel id="demo-multiple-chip-label">Select Display Metric</InputLabel>
        <Select
          labelId="demo-multiple-chip-label"
          id="demo-multiple-chip"
          value={selectedDisplayMetric}
          onChange={handleChange}
          input={<OutlinedInput id="select-multiple-chip" label="Select Display Metrics" />}
          renderValue={(selected) => (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
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

DatasetSelect.propTypes = {
  selectedMetrics: PropTypes.array.isRequired,
  selectedDisplayMetric: PropTypes.string.isRequired,
  setSelectedDisplayMetric: PropTypes.func.isRequired
};