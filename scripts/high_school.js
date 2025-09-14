import fs from "node:fs";
import PDLJS from 'peopledatalabs';
import { parse } from "csv-parse/sync";
import pkg from 'csvjson';
import dotenv from "dotenv";
const { toCSV } = pkg;
dotenv.config();
const PDLClient = new PDLJS({ apiKey: process.env.PEOPLE_DATA_LABS_API_KEY });

const filterSchools = () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../original_data/us-public-schools.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
        relax_quotes: true,
        delimiter: ";"
    });

    // Filter to only get high schools
    const highschools = [];
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { LEVEL_, NAME, WEBSITE } = record;
        if (LEVEL_ === "HIGH") {
            highschools.push({
                name: NAME,
                website: WEBSITE,
                type: "seconday"
            });
        }
    }

    const highschoolCSV = toCSV(highschools, { headers: 'key' });
    fs.writeFileSync("../test/highschool_output.csv", highschoolCSV, "utf-8");
}

const filterPrivateSchools = () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../original_data/us-private-schools.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
        relax_quotes: true,
        delimiter: ";"
    });

    // Filter to only get high schools
    const highschools = [];
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { NAME, WEBSITE } = record;
        highschools.push({
            name: NAME,
            website: WEBSITE,
            type: "seconday"
        });
    }

    const highschoolCSV = toCSV(highschools, { headers: 'key' });
    fs.writeFileSync("../test/private_highschool_output.csv", highschoolCSV, "utf-8");
}

const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const getPrivateSchoolData = async () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../test/private_highschool_output.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
        relax_quotes: true
    });

    // Filter to only get high schools
    const validHighSchools = [];
    const invalidHighSchools = [];
    const otherHighSchools = [];
    let count = 0;
    
    for (let i = 0; i < records.length; i++) {
        console.log(i);
        if (count === 100) {
            await sleep(60000);
            count = 0;
        }
        const record = records[i];
        const { name, website } = record;
        if (website === "NOT AVAILABLE") {
            try {
                const response = await PDLClient.school.cleaner({
                    name: name
                });
                if (response.status === 200) {
                    const pdl_name = response.name;
                    validHighSchools.push({
                        display_name: name,
                        pdl_name: pdl_name,
                        website: website,
                        type: "secondary"
                    })
                } else if (response.status === 404) {
                    invalidHighSchools.push(record);
                } else {
                    otherHighSchools.push(record);
                }
            } catch (err) {
                otherHighSchools.push(record);
            }
        } else {
            try {
                const response = await PDLClient.school.cleaner({
                    website: website
                });
                if (response.status === 200) {
                    const pdl_name = response.name;
                    validHighSchools.push({
                        display_name: name,
                        pdl_name: pdl_name,
                        website: website,
                        type: "secondary"
                    })
                } else if (response.status === 404) {
                    invalidHighSchools.push(record);
                } else {
                    otherHighSchools.push(record);
                }
            } catch (err) {
                otherHighSchools.push(record);
            }
        }
    }

    const otherCSV = toCSV(otherHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/other_private_hs.csv", otherCSV, "utf-8");
    const invalidCSV = toCSV(invalidHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/invalid_private_hs.csv", invalidCSV, "utf-8");
    const validCSV = toCSV(validHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/valid_private_hs.csv", validCSV, "utf-8");
}

const filterSchoolsWithValidWebsites = () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../test/highschool_output.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
        relax_quotes: true
    });

    // Filter to only get high schools
    const highschools = [];
    const missingWebsites = [];
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { name, website } = record;
        if (website !== "NOT AVAILABLE") {
            highschools.push({
                name: name,
                website: website,
                type: "secondary"
            });
        } else {
            missingWebsites.push({
                name: name,
                website: website,
                type: "secondary"
            });
        }
    }

    const highschoolCSV = toCSV(highschools, { headers: 'key' });
    fs.writeFileSync("../test/valid_website_highschools_public.csv", highschoolCSV, "utf-8");
    const missingCSV = toCSV(missingWebsites, { headers: 'key' });
    fs.writeFileSync("../test/missing_website_highschools_public.csv", missingCSV, "utf-8");
}

const getValidCleanSchoolData = async () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../test/invalid_highschool.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
    });

    // Filter to only get high schools
    const otherHighSchools = [];
    const invalidHighSchools = [];
    const validHighSchools = [];
    const start = 0;
    const end = Math.min(records.length - 1, 1000);
    let count = 0;
    for (let i = start; i <= records.length - 1; i++) {
        console.log(i);
        if (count === 100) {
            await sleep(60000);
            count = 0;
        }
        const record = records[i];
        const { name, website } = record;
        try {
             const response = await PDLClient.school.cleaner({
                name: name
            });
            if (response.status === 200) {
                const pdl_name = response.name;
                validHighSchools.push({
                    display_name: name,
                    pdl_name: pdl_name,
                    website: website,
                    type: "secondary"
                })
            } else if (response.status === 404) {
                invalidHighSchools.push(record);
            } else {
                otherHighSchools.push(record);
            }
        } catch (err) {
            invalidHighSchools.push(record);
        }
        count++;
    }

    const otherCSV = toCSV(otherHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/other_highschool_2.csv", otherCSV, "utf-8");
    const invalidCSV = toCSV(invalidHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/invalid_highschool_2.csv", invalidCSV, "utf-8");
    const validCSV = toCSV(validHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/valid_highschool_2.csv", validCSV, "utf-8");
}

const getCleanSchoolData = async () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../test/highschool_output.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
    });

    // Filter to only get high schools
    const otherHighSchools = [];
    const invalidHighSchools = [];
    const validHighSchools = [];
    const start = 0;
    const end = Math.min(records.length - 1, 1000);
    let count = 0;
    for (let i = 1001; i <= records.length - 1; i++) {
        console.log(i);
        if (count === 100) {
            await sleep(60000);
            count = 0;
        }
        const record = records[i];
        const { name, website } = record;
        try {
            if (website !== "NOT AVAILABLE") {
                const response = await PDLClient.school.cleaner({
                    website: website
                });
                if (response.status === 200) {
                    const pdl_name = response.name;
                    validHighSchools.push({
                        display_name: name,
                        pdl_name: pdl_name,
                        website: website,
                        type: "secondary"
                    })
                } else if (response.status === 404) {
                    invalidHighSchools.push(record);
                } else {
                    otherHighSchools.push(record);
                }
            } else {
                invalidHighSchools.push(record);
            }
        } catch (err) {
            invalidHighSchools.push(record);
        }
        count++;
    }

    const otherCSV = toCSV(otherHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/other_highschool.csv", otherCSV, "utf-8");
    const invalidCSV = toCSV(invalidHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/invalid_highschool.csv", invalidCSV, "utf-8");
    const validCSV = toCSV(validHighSchools, { headers: 'key' });
    fs.writeFileSync("../test/valid_highschool.csv", validCSV, "utf-8");
}

const cleanUpCSV = () => {
    // Read CSV file of careers
    const csvContent = fs.readFileSync("../test/valid_private_hs.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
        quote: true
    });
    // Take the pdl_name and prettify it to be the display name (to ensure consistency)
    const cleanedUpData = [];
    const setOfHighschools = new Set();
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { pdl_name, website, type } = record;
        let new_display_name = [];
        for (const word of pdl_name.split(" ")) {
            new_display_name.push(`${word.substring(0, 1).toUpperCase()}${word.substring(1)}`);
        }
        new_display_name = new_display_name.join(" ");
        if (!setOfHighschools.has(new_display_name)) {
            setOfHighschools.add(new_display_name);
            cleanedUpData.push({
                display_name: new_display_name,
                pdl_name: pdl_name,
                website: website,
                type: type
            });
        }
    }
    // Save results to new CSV
    const validCSV = toCSV(cleanedUpData, { headers: 'key' });
    fs.writeFileSync("../parsed_data/clean_valid_private_highschools.csv", validCSV, "utf-8");
}

// filterSchools();
// filterSchoolsWithValidWebsites();
// getCleanSchoolData();
// getValidCleanSchoolData();
cleanUpCSV();
// filterPrivateSchools();
// getPrivateSchoolData();