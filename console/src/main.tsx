import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { HashRouter } from "react-router-dom";
import { Toaster } from "sonner";
import App from "./App";
import "./styles.css";

createRoot(document.getElementById("root")!).render(<StrictMode><HashRouter><App /><Toaster position="bottom-right" richColors /></HashRouter></StrictMode>);
