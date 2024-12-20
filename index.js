const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const { google } = require("googleapis");

// Google Sheets credentials
const SPREADSHEET_ID = "14uxRX0IVzh8zEVX-F0qk5aybQynkdoIHPscWsX079mg";
const SECOND_SPREADSHEET_ID = "1hUVmRCy1JhafeWrC-qShT0QjofyxbKdJiwBKQqfTPVw";
const SHEET_NAME = "Dataset";
const CLIENT_EMAIL = "86540205027-compute@developer.gserviceaccount.com";
const PRIVATE_KEY =
  "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDFq5yGdxuoo6Zh\nemZrvMh2R6AG2h7vluAiKKwdmzG9SW3yqzxgHNx6QVn0iufLkS9qlex6pIsVFtkL\nADhzOB/1NEMtU7sI0WPeuEQcsYWkDcwU8zTMXDHrynzyk1lEoueyUzXbrDX9sOgj\nIlKoW36rvmsUGv5IM5zrgMFrN6lyBOraYXg7Z6uuwwLB3c+emGRfW/bGWZTd78sG\ncAEffwplwi1ZfMUDB8iDlAXYje8bY6e2tb3mE4w952yTfu57P9k9vRCC/4fOKyC2\nBWiPhKk6Foy7kJfGWttoDl6EQtIWZCZNoic/qVdalR1CaXiZfWSoD9aBA3GGE6E5\nQpmItuFTAgMBAAECggEAP2G1gNA0SDChKuRqbuVLidGRmGDcRoqkN4+/EIcwvbcL\n0CHj7BWVBIZr56Oai4V0JMMJ3pFgH1UCJyrc7uTyKlelqqUMddleIo4HGQQ7C814\nwMbzCQwO3cJBqi5hE0cA8AcKX/OqJDxTUuCwjFc6GKun/fxhyJj0GfdhDZX9eRdV\nmhJ6qEQICkRz6rIWPTqgKgz4sIl9sCVfHfPxI7c0yyuNbVA9T/buXc09GITd0dlo\nXF6FIlJ40vrT5iPKKk8x1Oqc7y5B08KzH2XlmkmsMXDYrgwrTZVS2cB09M8Vvhw7\n+FP+xRVhXe2o0LQ66NLXCP/LzU/HMpSgeUpLiVqCkQKBgQD4I8n+55znBUm7P6UE\nZCtQxsTXxSSsW3KCCwQDutIlh71MGQAFB/I9uJs7E4hQxxvi7p2YksQvySAGd9es\nGi9ao6ei2BJ/CosqLaZw4obwPEsZ24i8l4ZkgNN2x/0eY7UTeztNj9UA+JSNaRhK\n7vcd4Hth/PtXzvEP/mmr1Sw09wKBgQDL7o5zOiCsqLyfA0He2aOQPNfl2eE8sbg7\nd8pwQK86EOmoQJa9TFZloLQxyVv8u9b7XH0xOsFTRwB5g9D3zC3zIw6CuKcxVMiL\nOrVNx0Pc3fY1E0igIDpT+bs5u0wUKuyLcdhHb8wSgtuRNXnfRaIgh2rhEkcmbevU\n0Y7fxN1LhQKBgQCOkVF/aWeWvE0OjpSrHzpb4Lg8GILnnGsAGIAn/HC9K24xiLOg\nMF6X47ccjC5n6t401lAp1zltEyuZS8XYlfrbIugwAeEuqMooY64bcauB38JuitnX\nMV//4pycxG7DxRnGpaj++UKymiAP13AjrYTB37ZEKGvomXebbtsb5RDPJQKBgFzy\nV76w6Z+IMKAQ7f7SFzhwYr6CNaRiI+QGzx+me7btanGjLpEMr/wV6MsgSWrBelSK\nbQz5CJAaNl8r8xxd1TuR6NUvsBRN3jGHCoduoMGT8Nlz9o/04GDR28GOWjh+790G\ngFre25Y8SjK+utNGe4Rz9AStPfyH8QrIkGPw6CO5AoGBAOpZckcqm1ztAfoUFcx9\n17ma8BCFhu4j31Yov55xLmLVVjXjGRPx8DXRlWwSP2WqrIahOdfW0yxfvEkXCvDX\n3FIN6V6NU0uSiBXzRicAaM/JO9SZQXqrgJJMks8uaDVRXO2zUHARQEZD3cE/B7vA\nTCHHOhTmb22Vvo8Gw2yQTSCP\n-----END PRIVATE KEY-----\n";

// Authenticate with Google Sheets API
const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: CLIENT_EMAIL,
    private_key: PRIVATE_KEY.replace(/\\n/g, "\n"),
  },
  scopes: [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
  ],
});

const sheets = google.sheets({ version: "v4", auth });
const drive = google.drive({ version: "v3", auth });

// Folder ID for 'Autoloan File'
const FOLDER_ID = "1-To7PNavfAq_epklkKDGOzcGwoFHUgJp";

// Find the latest CSV file in a specific Google Drive folder
const findLatestFileInDriveFolder = async (folderId) => {
  try {
    const res = await drive.files.list({
      q: ` '${folderId}' in parents and mimeType='text/csv'`,
      orderBy: "modifiedTime desc",
      pageSize: 1,
      fields: "files(id, name, mimeType, modifiedTime)",
    });

    const files = res.data.files;
    if (files.length === 0) {
      console.error("No CSV files found in the specified folder");
      return null;
    }
    return files[0];
  } catch (error) {
    console.error("Error finding file in folder:", error);
    return null;
  }
};

// Download a file from Google Drive
const downloadCSVFile = async (fileId, destination) => {
  try {
    const res = await drive.files.get(
      { fileId, alt: "media" },
      { responseType: "stream" },
    );
    return new Promise((resolve, reject) => {
      const dest = fs.createWriteStream(destination);
      res.data
        .on("end", () => {
          console.log("File downloaded successfully");
          resolve();
        })
        .on("error", (err) => {
          console.error("Error downloading file:", err);
          reject(err);
        })
        .pipe(dest);
    });
  } catch (error) {
    console.error("Error downloading file:", error);
    throw error;
  }
};

// Read data from CSV file
const readCSVData = (filePath) => {
  return new Promise((resolve, reject) => {
    const data = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => data.push(Object.values(row)))
      .on("end", () => resolve(data))
      .on("error", (error) => reject(error));
  });
};

// Convert data types based on column index
const convertDataTypes = (data) => {
  return data.map((row) =>
    row.map((cell, index) => {
      if (typeof cell === "string") {
        // Remove single quotes if present
        if (cell.startsWith("'") && cell.endsWith("'")) {
          cell = cell.slice(1, -1);
        }

        if (index === 5 || index === 6) {
          // Columns F and G: Convert to number if possible
          const num = parseFloat(cell);
          if (!isNaN(num)) return num;
        }
      }
      return cell;
    }),
  );
};

// Update sheet data and format columns F and G
const updateSheetsData = async (data) => {
  try {
    // Hapus data dari baris 2 ke bawah di sheet pertama
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A2:M`,
    });

    // Tulis data baru mulai dari baris 2 di sheet pertama
    const range = `${SHEET_NAME}!A2`;
    const resource = { values: data };
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range,
      valueInputOption: "USER_ENTERED",
      resource,
    });

    // Update data ke sheet kedua (ID berbeda)
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SECOND_SPREADSHEET_ID,
      range: `${SHEET_NAME}!A2:M`,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SECOND_SPREADSHEET_ID,
      range,
      valueInputOption: "USER_ENTERED",
      resource,
    });

    console.log("Data berhasil ditulis ke kedua Google Sheets.");
  } catch (error) {
    console.error("Error saat memperbarui data di Google Sheets:", error);
  }
};

// Memproses CSV dan mengupdate kedua sheet
const processCSV = async () => {
  try {
    const file = await findLatestFileInDriveFolder(FOLDER_ID);
    if (!file) return;

    const tempFilePath = path.join(__dirname, "temp.csv");
    await downloadCSVFile(file.id, tempFilePath);

    const csvData = await readCSVData(tempFilePath);
    const convertedData = convertDataTypes(csvData);

    await updateSheetsData(convertedData);
    fs.unlinkSync(tempFilePath); // Hapus file sementara setelah diproses
  } catch (error) {
    console.error("Error processing CSV file:", error);
  }
};

// Eksekusi fungsi utama
processCSV();