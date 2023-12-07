import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import axios from "axios";
import { OutlinedInput, Box, LinearProgress } from "@mui/material";

export default function DatasetSelect(props) {
  const [errors, setErrors] = useState(false);
  const [loading, setLoading] = useState(true);
  const { datasets, setDatasets, selectedDataset, setSelectedDataset } = props;

  useEffect(() => {
    const fetchDatasets = () => {
      setLoading(true);
      axios
        .get(`http://localhost:8000/datasets/`)
        .then((res) => {
          // console.log(res.data);
          setDatasets(Object.keys(res.data));
          setLoading(false);
        })
        .catch(function (err) {
          console.log(err);
          setErrors(err);
          setLoading(false);
        });
    };
    fetchDatasets();
  }, [setDatasets, setLoading, setErrors]);

  const handleChange = (event) => {
    setSelectedDataset(event.target.value);
  };

  return (
    <div>
      {!loading ? (
        !errors ? (
          <FormControl sx={{ display: "flex" }}>
            <InputLabel id="demo-multiple-chip-label">
              Select Dataset
            </InputLabel>
            <Select
              labelId="demo-multiple-chip-label"
              id="demo-multiple-chip"
              value={selectedDataset}
              onChange={handleChange}
              input={
                <OutlinedInput
                  id="select-multiple-chip"
                  label="Select Metrics"
                />
              }
              renderValue={(selected) => (
                <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
                  {selected}
                </Box>
              )}
              // MenuProps={MenuProps}
            >
              {datasets.map((name) => (
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
        ) : (
          { errors }
        )
      ) : (
        <LinearProgress />
      )}
    </div>
  );
}

DatasetSelect.propTypes = {
  datasets: PropTypes.array.isRequired,
  selectedDataset: PropTypes.string.isRequired,
  setSelectedDataset: PropTypes.func.isRequired,
  setDatasets: PropTypes.func.isRequired,
};
