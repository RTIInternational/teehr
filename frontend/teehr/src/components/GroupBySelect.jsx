import { useState, useEffect, useContext } from "react";
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
import DashboardContext from "../Context";

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
  const { fieldValues, setFieldValues } = useContext(DashboardContext);
  const [errors, setErrors] = useState(false);
  const [loading, setLoading] = useState(true);
  const {
    groupByFields,
    setGroupByFields,
    selectedGroupByFields,
    setSelectedGroupByFields,
    selectedDataset,
    includeSpatialData,
  } = useContext(DashboardContext);

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
            if (includeSpatialData) {
              setSelectedGroupByFields(
                res.data.filter((d) => d.name === "primary_location_id")
              );
            }
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
    const value = event.target.value;
    // On autofill we get a stringified value.
    const arr = typeof value === "string" ? value.split(",") : value;
    arr.forEach((field) => {
      if (
        field.name in fieldValues ||
        field.type === "TIMESTAMP" ||
        field.name === "primary_location_id"
      ) {
        return;
      }
      axios
        .post(
          `http://localhost:8000/datasets/${selectedDataset}/get_unique_field_values`,
          {
            field_name: field.name,
          }
        )
        .then((res) => {
          const options = res.data.map((o) => Object.values(o)[0]);
          setFieldValues((prev) => {
            return { ...prev, [field.name]: options };
          });
        })
        .catch(function (err) {
          console.log(err);
          setErrors(err);
        });
    });
    setSelectedGroupByFields(arr);
  };

  return (
    <div>
      {!loading ? (
        !errors ? (
          <FormControl sx={{ mt: 1, mb: 1, display: "flex" }}>
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
