import { useEffect, useState } from "react";

const DarkModeToggle = () => {
  const [theme, setTheme] = useState(() => {
    const storedTheme = localStorage.getItem("bsTheme");
    return storedTheme || "light";
  });

  useEffect(() => {
    // Set the data-bs-theme attribute on the html element *and* the nav
    document.documentElement.setAttribute("data-bs-theme", theme);
    const navElement = document.getElementById("nav-bar"); // Get the nav element
    if (navElement) {
      navElement.setAttribute("data-bs-theme", theme);
      if (theme === "dark") {
        navElement.classList.replace("navbar-light", "navbar-dark");
      } else {
        navElement.classList.replace("navbar-dark", "navbar-light");
      }
    }

    localStorage.setItem("bsTheme", theme);

    const contentElement = document.querySelector("#container");
    if (contentElement) {
      if (theme === "dark") {
        contentElement.classList.replace("content-light", "content-dark");
      } else {
        contentElement.classList.replace("content-dark", "content-light");
      }
    }

    // This is a very broad scope intended to catch all buttons using the button-outline syntax. Watch for scope
    // creep on this one, may need to be narrowed.
    const btnElements = document.querySelectorAll(".btn"); // Select ALL buttons

    btnElements.forEach((btnElement) => {
      // Loop through all buttons
      if (theme === "dark") {
        btnElement.classList.replace("btn-outline-dark", "btn-outline-light");
      } else {
        btnElement.classList.replace("btn-outline-light", "btn-outline-dark");
      }
    });

    const footerElement = document.querySelector("footer");
    if (footerElement) {
      if (theme === "dark") {
        footerElement.classList.replace("footer-light", "footer-dark");
      } else {
        footerElement.classList.replace("footer-dark", "footer-light");
      }
    }
  }, [theme]);

  const handleChange = (event) => {
    setTheme(event.target.checked ? "dark" : "light");
  };

  return (
    <div className="form-check form-switch color-mode">
      {" "}
      {/* Added color-mode class */}
      <input
        className="form-check-input"
        type="checkbox"
        id="darkModeSwitch"
        checked={theme === "dark"}
        onChange={handleChange}
      />
      <label className="form-check-label" htmlFor="darkModeSwitch" id="colorSwitchLabel">
        <i className={`bi bi-${theme === "dark" ? "moon-stars-fill" : "sun-fill"}`}></i>
      </label>
    </div>
  );
};

export default DarkModeToggle;
