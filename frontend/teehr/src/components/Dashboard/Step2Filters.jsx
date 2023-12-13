import { useContext, useState } from "react";
import { useForm, useFieldArray } from "react-hook-form";
import { Button, Box, Grid, IconButton } from "@mui/material";
import DashboardContext from "../../Context";
import FormSingleSelect from "../form-components/FormSingleSelect";
import FormTimePicker from "../form-components/FormTimePicker";
import FormInputText from "../form-components/FormInputText";
import useDashboardAPI from "../../hooks/useDashboardAPI";
import PropTypes from "prop-types";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import { nonListFields } from "./constants";

const castFieldValue = (value, fieldType) => {
  switch (fieldType) {
    case "FLOAT":
      return parseFloat(value);
    case "INTERVAL":
      return parseInt(value);
    case "BOOLEAN":
      return Boolean(value === "true" || value === true);
    default:
      return value;
  }
};

const FiltersSection = (props) => {
  const { onNext, onBack } = props;
  const { fetchOptionsForField, fetchStations } = useDashboardAPI();
  const {
    groupByFields,
    operatorOptions,
    fieldOptions,
    setFieldOptions,
    formData,
    setFormData,
    setData,
  } = useContext(DashboardContext);

  const {
    selectedDataset,
    selectedFilters,
    selectedGroupByFields,
    selectedMetrics,
    includeSpatialData,
  } = formData;

  const getFieldType = (fieldName) => {
    const field = groupByFields.find((field) => field.name === fieldName);
    return field ? field.type : "";
  };

  const [filterMetadata, setFilterMetadata] = useState(() => {
    return selectedFilters.reduce((obj, item, index) => {
      obj[index] = {
        type: getFieldType(item.column),
        column: item.column,
        options: fieldOptions[item.column],
      };
      return obj;
    }, {});
  });

  const onFilterFieldChange = async (newField, onChange, index) => {
    let options = null;
    if (!nonListFields.includes(newField)) {
      options = await getFieldOptions(newField);
    }
    setFilterMetadata((prev) => ({
      ...prev,
      [index]: {
        type: getFieldType(newField),
        column: newField,
        options: options,
      },
    }));
    onChange(newField);
  };

  const getFieldOptions = async (filter) => {
    if (!(filter in fieldOptions)) {
      const options = await fetchOptionsForField(filter, selectedDataset);
      setFieldOptions((prev) => ({ ...prev, [filter]: options }));
      return options;
    } else {
      return fieldOptions[filter];
    }
  };

  const { control, handleSubmit } = useForm({
    defaultValues: {
      filters: selectedFilters,
    },
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: "filters",
  });

  const onSubmit = async (data) => {
    setFormData((prev) => ({ ...prev, selectedFilters: data.filters }));
    const modifiedFilters = data.filters.map((filter) => {
      const fieldType = getFieldType(filter.column);
      return {
        ...filter,
        value: castFieldValue(filter.value, fieldType),
      };
    });
    await handleFetchData(modifiedFilters);
    onNext();
  };

  const handleFetchData = async (filters) => {
    let fields = [...selectedGroupByFields];
    if (
      formData.includeSpatialData &&
      !fields.includes("primary_location_id")
    ) {
      fields.push("primary_location_id");
    }
    return fetchStations(
      selectedDataset,
      fields,
      selectedMetrics,
      filters,
      includeSpatialData
    ).then((res) => {
      setData(res.data);
      return;
    });
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)}>
      <Grid container justifyContent="center">
        <Grid item xs={12} sm={8} md={10}>
          {fields.length === 0 && (
            <p>
              <em>No filters applied.</em>
            </p>
          )}
          {fields.length > 0 &&
            fields.map((item, index) => {
              return (
                <Box key={item.id} sx={{ display: "flex" }}>
                  <FormSingleSelect
                    name={`filters[${index}].column`}
                    control={control}
                    label={"Filter"}
                    options={groupByFields.map((field) => field.name) || []}
                    rules={{ required: "Required." }}
                    onChange={(e, fn) => onFilterFieldChange(e, fn, index)}
                  />
                  <FormSingleSelect
                    name={`filters[${index}].operator`}
                    control={control}
                    label={"Operator"}
                    options={Object.values(operatorOptions) || []}
                    rules={{ required: "Required." }}
                  />
                  {filterMetadata[index]?.type === "TIMESTAMP" && (
                    <FormTimePicker
                      name={`filters[${index}].value`}
                      control={control}
                      label="Value"
                      rules={{ required: "Required." }}
                      style={{ m: 0 }}
                    />
                  )}
                  {filterMetadata[index]?.type !== "TIMESTAMP" &&
                    !nonListFields.includes(filterMetadata[index]?.column) && (
                      <FormSingleSelect
                        name={`filters[${index}].value`}
                        control={control}
                        label={"Value"}
                        options={filterMetadata[index]?.options || []}
                        rules={{ required: "Required." }}
                      />
                    )}
                  {filterMetadata[index]?.type !== "TIMESTAMP" &&
                    nonListFields.includes(filterMetadata[index]?.column) && (
                      <FormInputText
                        name={`filters[${index}].value`}
                        control={control}
                        label={"Value"}
                        rules={{ required: "Required." }}
                      />
                    )}
                  <IconButton
                    onClick={() => {
                      remove(index);
                    }}
                  >
                    <DeleteOutlineOutlinedIcon />
                  </IconButton>
                </Box>
              );
            })}
        </Grid>
      </Grid>
      <Button
        onClick={() => {
          append({ column: "", operator: "", value: "" });
        }}
      >
        Add Filter
      </Button>
      <Box sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
        <Button color="inherit" onClick={onBack} sx={{ mr: 1 }}>
          Back
        </Button>
        <Box sx={{ flex: "1 1 auto" }} />
        <Button type="submit">Next</Button>
      </Box>
    </form>
  );
};

FiltersSection.propTypes = {
  onNext: PropTypes.func.isRequired,
  onBack: PropTypes.func.isRequired,
};

export default FiltersSection;
