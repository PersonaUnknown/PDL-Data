import fs from "node:fs";
import { PrismaClient } from "@prisma/client";
import { parse } from "csv-parse/sync";

const prisma = new PrismaClient();

const uploadLocations = async () => {
    const csvContent = fs.readFileSync("../parsed_data/uscities.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
    });
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, pdl_name, type } = record;
        const check = await prisma.PDLLocation.findFirst({
            where: {
                displayName: display_name
            }
        });
        if (!check) {
            const parsedType = type === "city" ? "CITY" : "STATE";
            await prisma.PDLLocation.create({
                data: {
                    displayName: display_name,
                    pdlName: pdl_name,
                    type: parsedType
                }
            });
        }
    }
}

const uploadColleges = async () => {
    const csvContent = fs.readFileSync("../parsed_data/colleges.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
    });
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, pdl_name, website, type } = record;
        const check = await prisma.PDLSchool.findFirst({
            where: {
                displayName: display_name
            }
        });
        if (!check) {
            await prisma.PDLSchool.create({
                data: {
                    displayName: display_name,
                    pdlName: pdl_name,
                    website: website,
                    type: "COLLEGE"
                }
            });
        }
    }
}

const uploadHighSchools = async () => {
    const csvContent = fs.readFileSync("../parsed_data/valid_highschool.csv", "utf-8");
    const records = parse(csvContent, {
        columns: true, // Treat the first row as headers
        skip_empty_lines: true, // Ignore empty lines
    });
    for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const { display_name, pdl_name, website, type } = record;
        const check = await prisma.PDLSchool.findFirst({
            where: {
                displayName: display_name
            }
        });
        if (!check) {
            await prisma.PDLSchool.create({
                data: {
                    displayName: display_name,
                    pdlName: pdl_name,
                    website: website,
                    type: "HIGH_SCHOOL"
                }
            });
        }
    }
}

// uploadLocations();
// uploadColleges();
uploadHighSchools();