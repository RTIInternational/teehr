// import * as React from 'react';
import { useState, useEffect } from "react";
import Box from "@mui/material/Box";
import PropTypes from "prop-types";
import { DataGrid } from "@mui/x-data-grid";

export default function DataGridDemo(props) {
  const { data } = props;
  const [columns, setColumns] = useState([]);
  const [rows, setRows] = useState([]);

  useEffect(() => {
    const getColumns = () => {
      if (data && data.length > 0) {
        const base = [
          {
            field: "id",
            headerName: "id",
            editable: false,
            flex: 0.25,
          },
        ];
        const headers = Object.keys(data[0]).map((c) => {
          return {
            field: c,
            headerName: c,
            editable: false,
            flex: 1,
          };
        });
        base.push(...headers);
        setColumns(headers.some((o) => o.field === "id") ? headers : base);
      }
    };
    getColumns();
  }, [data, setColumns]);

  useEffect(() => {
    const getRows = () => {
      if (data) {
        const arr = data.map((d, index) => {
          const obj = { ...d };
          if (!("id" in obj)) {
            obj["id"] = index;
          }
          return obj;
        });
        setRows(arr);
      }
    };
    getRows();
  }, [data, setRows]);

  return (
    data &&
    columns && (
      <Box sx={{ height: 400, width: "100%" }}>
        <DataGrid
          rows={rows}
          columns={columns}
          initialState={{
            pagination: {
              paginationModel: {
                pageSize: 5,
              },
            },
          }}
          pageSizeOptions={[5]}
          checkboxSelection
          disableRowSelectionOnClick
        />
      </Box>
    )
  );
}

DataGridDemo.propTypes = {
  data: PropTypes.array.isRequired,
};
