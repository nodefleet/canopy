import React, { useState, useEffect } from "react";
import { Spinner } from "react-bootstrap";

const CanaLog = ({ text }) => {
  const [logs, setLogs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);

  const parseLogs = (data) => {
    if (!data) return [];

    const lines = data.split("\n");
    const parsedLogs = lines.reduce((acc, line) => {
      let trimmedLine = line.trim();
      if (trimmedLine === "") return acc;

      /*
      Believe me when I say that you do not want to modify this parsing
      function at all. These steps need to run exactly in this order, and
      what appears to be an inefficient multi-stage regex match is literally
      the only approach that works.
      */

      // 1. Decode HTML entities (if present)
      trimmedLine = trimmedLine.replace(/&#(\d+);/g, (match, code) => String.fromCharCode(code));

      // 2. Remove ANSI escape codes FIRST
      const cleanedLine = trimmedLine.replace(/\x1b\[\d+m/g, "");

      // 3. Split by the *cleaned* timestamp (first 19 characters)
      const timestamp = cleanedLine.slice(0, 19).trim(); // Extract timestamp

      // 4. Extract msgtype and message
      const messageParts = cleanedLine
        .slice(19)
        .trim()
        .match(/^(DEBUG:|INFO:)\s(.*)$/);
      let msgtype = "";
      let message = cleanedLine.slice(19).trim(); // Default message if no match

      if (messageParts && messageParts.length === 3) {
        msgtype = messageParts[1];
        message = messageParts[2];
      }

      acc.push({ timestamp, msgtype, message });

      return acc;
    }, []);

    return parsedLogs;
  };

  useEffect(() => {
    if (!text) {
      // Check for null or undefined text
      setLogs([]);
      setIsLoading(true); // Set loading to true if no text
      return;
    }

    const newLogs = parseLogs(text);
    setLogs(newLogs);
    setIsLoading(false);
  }, [text]);

  if (isLoading) {
    // Render spinner if loading
    return (
      <div className="canalog-container d-flex justify-content-center align-items-center" style={{ height: "200px" }}>
        <Spinner animation="border" role="status">
          <span className="visually-hidden">Loading...</span>
        </Spinner>
      </div>
    );
  } else {
    return (
      <div className="canalog-container">
        {logs.map((log, index) => (
          <div key={index} className="canalog-row">
            <span className="canalog-label">{log.timestamp}</span>
            <span
              className={`canalog-msgtype ${log.msgtype === "DEBUG:" ? "canalog-debug" : log.msgtype === "INFO:" ? "canalog-info" : ""}`}
            >
              {log.msgtype}
            </span>
            <span className="canalog-message">{log.message}</span>
          </div>
        ))}
      </div>
    );
  }
};

export default CanaLog;
