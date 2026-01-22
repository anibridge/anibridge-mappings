import { render } from "hono/jsx/dom";
import { App } from "./components/App";
import "./style.css";

const root = document.getElementById("app");

if (!root) {
  throw new Error("Missing #app root element.");
}

render(<App />, root);
