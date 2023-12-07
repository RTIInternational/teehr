import { useEffect, useContext } from "react";
import PropTypes from "prop-types";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import { OutlinedInput, Box } from "@mui/material";
import axios from "axios";
import DashboardContext from "../Context";

export default function OperatorSelect(props) {
  const { operators, selectedDataset, setOperators, setLoading, setErrors } =
    useContext(DashboardContext);
  const { value, onChange } = props;

  useEffect(() => {
    const fetchFilterOperators = () => {
      if (selectedDataset) {
        setLoading(true);
        axios
          .get(
            `http://localhost:8000/datasets/${selectedDataset}/get_filter_operators`
          )
          .then((res) => {
            // console.log(res.data);
            setOperators(res.data);
            setLoading(false);
          })
          .catch(function (err) {
            console.log(err);
            setErrors(err);
            setLoading(false);
          });
      }
    };
    fetchFilterOperators();
  }, [selectedDataset, setLoading, setErrors, setOperators]);

  return (
    <FormControl sx={{ display: "flex" }}>
      <InputLabel id="demo-multiple-chip-label">Operator</InputLabel>
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
        // MenuProps={MenuProps}
      >
        {Object.values(operators).map((label) => (
          <MenuItem
            key={label}
            value={label}
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
  value: PropTypes.string,
  onChange: PropTypes.func.isRequired,
};
