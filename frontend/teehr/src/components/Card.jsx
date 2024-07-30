import { NavLink } from "react-router-dom";
import Card from "@mui/material/Card";
import CardActionArea from "@mui/material/CardActionArea";
import CardMedia from "@mui/material/CardMedia";
import PropTypes from "prop-types";
import AddchartOutlinedIcon from "@mui/icons-material/AddchartOutlined";

const ClickableCardPropTypes = {
  title: PropTypes.string.isRequired,
  image: PropTypes.func,
  route: PropTypes.string.isRequired,
};

function ClickableCard({ title, image, route }) {
  return (
    <NavLink to={route} style={{ textDecoration: "none" }}>
      <Card
        sx={{
          minWidth: 250,
          minHeight: 200,
          height: "100%",
          padding: 0,
          display: "flex",
          displayDirection: "column",
          flexGrow: 1,
        }}
      >
        <CardActionArea>
          {image && (
            <CardMedia
              component="img"
              height="140"
              image={image}
              alt={title}
              sx={{ flexGrow: 1 }}
            />
          )}
          {!image && <AddchartOutlinedIcon sx={{ flexGrow: 1 }} />}
        </CardActionArea>
      </Card>
    </NavLink>
  );
}

ClickableCard.propTypes = ClickableCardPropTypes;

export default ClickableCard;
