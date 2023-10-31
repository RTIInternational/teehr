import React from "react";
import {
  Box,
  AppBar,
  Toolbar,
  Typography,
  Drawer,
  CssBaseline,
  Grid,
  Paper,
} from "@mui/material";
import ClickableCard from "./Card";
import teehrLogo from "../assets/teehr.png";
import cirohLogo from "../assets/ciroh.png";

const appBarStyle = { bgcolor: "#2E8BC0" };

const drawerWidth = 240;

const drawerStyle = {
  width: drawerWidth,
  flexShrink: 0,
  "& .MuiDrawer-paper": {
    width: drawerWidth,
    boxSizing: "border-box",
    mt: "64px",
    bgcolor: "#0C2D48",
  },
  display: { xs: "none", sm: "none", md: "block" },
};

const sectionHeaderStyle = {
  height: "100%",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
};

const logoStyle = {
  "& img": {
    height: { xs: "80px", sm: "120px" },
  },
};

const logos = [
  { src: cirohLogo, alt: "CIROH Logo" },
  { src: teehrLogo, alt: "TEEHR Logo" },
];

function createNavlink(title, description, to) {
  return {
    title,
    description,
    to,
  };
}

const navLinksPostEvent = [
  createNavlink("Event Dashboard Title", "Description", "/map-dashboard"),
  createNavlink("Event Dashboard Title", "Description", "/map-dashboard"),
];

const navLinksHistoricalSimulation = [
  createNavlink("Simulation Dashboard Title", "Description", "/map-dashboard"),
  createNavlink("Simulation Dashboard Title", "Description", "/map-dashboard"),
];

const navLinksFIM = [
  createNavlink("FIM Dashboard Title", "Description", "/map-dashboard"),
  createNavlink("Simulation Dashboard Title", "Description", "/map-dashboard"),
];

const sections = {
  "Post-Event": { links: navLinksPostEvent, cardGridSize: 5 },
  "Historical Simulation": {
    links: navLinksHistoricalSimulation,
    cardGridSize: 5,
  },
  FIM: { links: navLinksFIM, cardGridSize: 5 },
};

export default function HomePage() {
  return (
    <Box sx={{ display: "flex" }}>
      <CssBaseline />
      <AppBar position="fixed" sx={appBarStyle}>
        <Toolbar>
          <Typography>Homepage Text</Typography>
        </Toolbar>
      </AppBar>
      <Drawer sx={drawerStyle} variant="permanent" anchor="left"></Drawer>
      <Box component="main" sx={{ flexGrow: 1, bgcolor: "background.default" }}>
        <Toolbar />
        <Grid container spacing={4}>
          {logos.map((logo, index) => (
            <Grid item xs={12} sm={3} key={index} sx={logoStyle}>
              <img src={logo.src} alt={logo.alt} />
            </Grid>
          ))}
          <Grid item xs={12} sm={6} />
          {Object.entries(sections).map(
            ([sectionName, sectionDetails], sectionIndex) => (
              <React.Fragment key={sectionIndex}>
                <Grid item xs={12} sm={2}>
                  <Paper elevation={0} sx={sectionHeaderStyle}>
                    <Typography variant="h6">{sectionName}</Typography>
                  </Paper>
                </Grid>
                {sectionDetails.links.map((link, index) => (
                  <Grid
                    item
                    xs={12}
                    sm={sectionDetails.cardGridSize}
                    key={index}
                  >
                    <ClickableCard title={link.title} route={link.to} />
                  </Grid>
                ))}
              </React.Fragment>
            )
          )}
        </Grid>
      </Box>
    </Box>
  );
}
