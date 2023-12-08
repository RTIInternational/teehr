import {
  createBrowserRouter,
  createRoutesFromElements,
  Route,
} from "react-router-dom";
import Dashboard from "./components/Dashboard";
import HomePage from "./components/Home";

const router = createBrowserRouter(
  createRoutesFromElements(
    <Route path={`/`}>
      <Route index element={<HomePage />} />
      <Route path="map-dashboard" element={<Dashboard />} />
    </Route>
  )
);

export default router;
