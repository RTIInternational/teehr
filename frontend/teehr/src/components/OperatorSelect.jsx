import PropTypes from "prop-types";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import { OutlinedInput, Box } from "@mui/material";

const filterOperatorEnum = {
  EQ: "=",
  GT: ">",
  LT: "<",
  GTE: ">=",
  LTE: "<=",
  ISLIKE: "like",
  ISIN: "in",
};

export default function OperatorSelect(props) {
  const { selectedOperator, setSelectedOperator } = props;

  const handleChange = (event) => {
    setSelectedOperator(event.target.value);
  };

  return (
    <FormControl sx={{ m: 1, display: "flex" }}>
      <InputLabel id="demo-multiple-chip-label">Operator</InputLabel>
      <Select
        labelId="demo-multiple-chip-label"
        id="demo-multiple-chip"
        value={selectedOperator}
        onChange={handleChange}
        input={
          <OutlinedInput
            id="select-multiple-chip"
            label="Select Display Metrics"
          />
        }
        renderValue={(selected) => (
          <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
            {filterOperatorEnum[selected]}
          </Box>
        )}
        // MenuProps={MenuProps}
      >
        {Object.entries(filterOperatorEnum).map(([key, label]) => (
          <MenuItem
            key={key}
            value={key}
            // style={getStyles(name, selectedDataset, theme)}
          >
            {label}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
}

OperatorSelect.propTypes = {
  selectedOperator: PropTypes.string,
  setSelectedOperator: PropTypes.func.isRequired,
};
