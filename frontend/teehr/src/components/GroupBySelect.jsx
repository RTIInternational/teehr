import { useState, useEffect } from "react";
import { useTheme } from "@mui/material/styles";
import PropTypes from "prop-types";
import Box from "@mui/material/Box";
import OutlinedInput from "@mui/material/OutlinedInput";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select from "@mui/material/Select";
import Chip from "@mui/material/Chip";
import LinearProgress from "@mui/material/LinearProgress";
import axios from "axios";

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

function getStyles(name, personName, theme) {
  return {
    fontWeight:
      personName.indexOf(name) === -1
        ? theme.typography.fontWeightRegular
        : theme.typography.fontWeightMedium,
  };
}

export default function GroupBySelect(props) {
  const theme = useTheme();
  const [errors, setErrors] = useState(false);
  const [loading, setLoading] = useState(true);
  const {
    groupByFields,
    setGroupByFields,
    selectedGroupByFields,
    setSelectedGroupByFields,
    selectedDataset,
  } = props;

  useEffect(() => {
    const fetchMetricFields = () => {
      if (selectedDataset && selectedDataset !== "") {
        axios
          .get(
            `http://localhost:8000/datasets/${selectedDataset}/get_data_fields`
          )
          .then((res) => {
            // console.log(res.data);
            setGroupByFields(res.data);
            setLoading(false);
          })
          .catch(function (err) {
            console.log(err);
            setErrors(err);
            setLoading(false);
          });
      }
    };
    fetchMetricFields();
  }, [selectedDataset, setGroupByFields, setLoading, setErrors]);

  const handleChange = (event) => {
    const {
      target: { value },
    } = event;
    setSelectedGroupByFields(
      // On autofill we get a stringified value.
      typeof value === "string" ? value.split(",") : value
    );
  };

  return (
    <div>
      {!loading ? (
        !errors ? (
          <FormControl sx={{ m: 1, display: "flex" }}>
            <InputLabel id="demo-multiple-chip-label">
              Group By Fields
            </InputLabel>
            <Select
              labelId="demo-multiple-chip-label"
              id="demo-multiple-chip"
              multiple
              value={selectedGroupByFields}
              onChange={handleChange}
              input={
                <OutlinedInput
                  id="select-multiple-chip"
                  label="Group By Fields"
                />
              }
              renderValue={(selected) => (
                <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
                  {selected.map((obj) => (
                    <Chip key={obj.name} label={obj.name} />
                  ))}
                </Box>
              )}
              MenuProps={MenuProps}
            >
              {groupByFields.map((obj) => (
                <MenuItem
                  key={obj.name}
                  value={obj}
                  style={getStyles(obj.name, selectedGroupByFields, theme)}
                >
                  {obj.name}
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

GroupBySelect.propTypes = {
  groupByFields: PropTypes.array.isRequired,
  setGroupByFields: PropTypes.func.isRequired,
  selectedGroupByFields: PropTypes.array.isRequired,
  setSelectedGroupByFields: PropTypes.func.isRequired,
  selectedDataset: PropTypes.string.isRequired,
};
