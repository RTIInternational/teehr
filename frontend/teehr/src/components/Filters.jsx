import { useContext } from "react";
import { Box, Button, Typography } from "@mui/material";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import DashboardContext from "../Context";
import Filter from "./SingleFilter";

export default function Filters() {
  const { filters, setFilters } = useContext(DashboardContext);

  const addNewFilter = () => {
    setFilters([...filters, { column: "", operator: "", value: "" }]);
  };

  const updateFilter = (index, key, value) => {
    const newFilters = [...filters];
    newFilters[index][key] = value;
    setFilters(newFilters);
  };

  const deleteFilter = (indexToRemove) => {
    const newFilters = filters.filter((_, index) => index !== indexToRemove);
    setFilters(newFilters);
  };

  return (
    <Box>
      <Typography>Filters</Typography>
      {filters.map((filter, index) => (
        <div key={index} style={{ marginBottom: "8px", display: "flex" }}>
          <Filter
            selectedGroupByField={filter.column || ""}
            selectedOperator={filter.operator || ""}
            value={filter.value.toString() || ""}
            setSelectedGroupByField={(value) =>
              updateFilter(index, "column", value)
            }
            setSelectedOperator={(value) =>
              updateFilter(index, "operator", value)
            }
            setValue={(value) => updateFilter(index, "value", value)}
            deleteFilter={() => deleteFilter(index)}
          />
        </div>
      ))}
      <Button variant="standard" onClick={addNewFilter} color="grey">
        <AddOutlinedIcon />
      </Button>
    </Box>
  );
}
