import fs from "node:fs";
import PDLJS from 'peopledatalabs';
import { parse } from "csv-parse/sync";
import pkg from 'csvjson';
import dotenv from "dotenv";
const { toCSV } = pkg;
dotenv.config();

const PDLClient = new PDLJS({ apiKey: process.env.PEOPLE_DATA_LABS_API_KEY });

/**
 * Parses through a list of careers to check
 * Grab an existing subset of careers from target CSV file
 * Then use People Data Labs to generate a subset of valid careers
 * and a list of invalid careers after this check
 */
const separateValidAndInvalidCareers = () => {
    // Read list of careers
    const invalidCareerSet = new Set();
    const invalidCareerList = fs.readFileSync("../test/test.txt", 'utf8');
    const invalidCareers = invalidCareerList.split("\n");
    for (const career of invalidCareers) {
        const colonIndex = career.indexOf(" : ");
        const name = career.substring(0, colonIndex).trim();
        invalidCareerSet.add(name);
    }

    // Read CSV file of careers
    const csvContent = fs.readFileSync("../test/colleges.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true // Ignore empty lines
    });

    const website_not_available = [];
    const missingCareers = []; // Careers that still need to be checked
    const validCareers = []; // Careers that aren't in the invalid career list (assume it's valid)
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, website } = record;
        if (invalidCareerSet.has(display_name)) {
            if (website === "NOT AVAILABLE") {
                website_not_available.push(display_name);
            } else {
                missingCareers.push(record);
            }
        } else {
            validCareers.push(record);
        }
    }

    const validCareerCSV = toCSV(validCareers, { headers: 'key' });
    fs.writeFileSync("../test/output.csv", validCareerCSV, "utf-8");
    const missingCareerCSV = toCSV(missingCareers, { headers: 'key' });
    fs.writeFileSync("../test/missing.csv", missingCareerCSV, "utf-8");
    const websiteUnavailableTXT = website_not_available.join("\n");
    fs.writeFileSync("../test/website_unavailable.txt", websiteUnavailableTXT, "utf-8");
}

/**
 * Get school data from PDL based on entries that have a valid website
 * and save to a new CSV file
 */
const getCleanSchoolData = async () => {
    // Read list of careers
    const csvContent = fs.readFileSync("../test/missing.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true // Ignore empty lines
    });

    
    const badResponses = [];
    const goodResponses = [];
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, website, type } = record;
        try {
            const response = await PDLClient.school.cleaner({
                website: website
            });

            if (response.status === 200) {
                const { name } = response;
                goodResponses.push({
                    display_name: display_name,
                    pdl_name: name,
                    website: website,
                    type: type
                });
            } else {
                badResponses.push(record);
            }
        } catch (err) {
            badResponses.push(record);
        }
    }
    const goodResponseCSV = toCSV(goodResponses, { headers: 'key' });
    fs.writeFileSync("../test/good.csv", goodResponseCSV, "utf-8");
    const badResponseCSV = toCSV(badResponses, { headers: 'key' });
    fs.writeFileSync("../test/bad.csv", badResponseCSV, "utf-8");
}

const getCleanSchoolDataFollowup = async () => {
    // Read list of careers
    const csvContent = fs.readFileSync("../test/bad.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true // Ignore empty lines
    });

    
    const badResponses = [];
    const goodResponses = [];
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, pdl_name, website, type } = record;
        try {
            const response = await PDLClient.school.cleaner({
                name: pdl_name
            });

            if (response.status === 200) {
                const { name } = response;
                goodResponses.push({
                    display_name: display_name,
                    pdl_name: name,
                    website: website,
                    type: type
                });
            } else {
                badResponses.push(record);
            }
        } catch (err) {
            badResponses.push(record);
        }
    }
    const goodResponseCSV = toCSV(goodResponses, { headers: 'key' });
    fs.writeFileSync("../test/good_2.csv", goodResponseCSV, "utf-8");
    const badResponseCSV = toCSV(badResponses, { headers: 'key' });
    fs.writeFileSync("../test/bad_2.csv", badResponseCSV, "utf-8");
}

const finalCleanSchoolDataFollowup = async () => {
    // Read list of careers
    const csvContent = fs.readFileSync("../test/bad_2.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true // Ignore empty lines
    });

    
    const badResponses = [];
    const goodResponses = [];
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, pdl_name, website, type } = record;
        try {
            const response = await PDLClient.school.cleaner({
                name: pdl_name
            });

            if (response.status === 200) {
                const { name } = response;
                goodResponses.push({
                    display_name: display_name,
                    pdl_name: name,
                    website: website,
                    type: type
                });
            } else {
                badResponses.push(record);
            }
        } catch (err) {
            badResponses.push(record);
        }
    }
    const goodResponseCSV = toCSV(goodResponses, { headers: 'key' });
    fs.writeFileSync("../test/good_3.csv", goodResponseCSV, "utf-8");
    const badResponseCSV = toCSV(badResponses, { headers: 'key' });
    fs.writeFileSync("../test/bad_3.csv", badResponseCSV, "utf-8");
}

// separateValidAndInvalidCareers();
// getCleanSchoolData();
// getCleanSchoolDataFollowup();
finalCleanSchoolDataFollowup();