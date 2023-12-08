// import * as React from 'react';
import { useState, useEffect } from 'react'
import Box from '@mui/material/Box';
import PropTypes from "prop-types";
import { DataGrid } from '@mui/x-data-grid';


export default function DataGridDemo(props) {

  const [columns, setColumns] = useState(null);
  const [rows, setRows] = useState(null);

  const { data } = props;

  // function getRowId(row) {
  //   return row.primary_location_id;
  // }

  useEffect(() => {
    const getColumns = () => {
      if (data) {
        const base = [
          {
            field: "id",
            headerName: "id",
            editable: false,
          }
        ]
        const arr = Object.keys(data.features[0].properties).map((c) => {
          return {
            field: c,
            headerName: c,
            editable: false,
          }
        })
        base.push(...arr)
        // console.log(arr)
        setColumns(base)
      }
    }
    getColumns()
  }, [data, setColumns])

  useEffect(() => {
    const getRows= () => {
      if (data) {
        const arr = data.features.map((feat) => {
          const obj = feat.properties
          obj["id"] = feat.id
          return obj
        })
        console.log(arr)
        setRows(arr)
      }
    }
    getRows()
  }, [data, setRows])


  return (
    (data && columns) && (

      <Box sx={{ height: 400, width: '100%' }}>
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
          // getRowId={getRowId}
        />
      </Box>

    )
  );
}

DataGridDemo.propTypes = {
  data: PropTypes.object.isRequired
};