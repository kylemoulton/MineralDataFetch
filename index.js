const   axios =     require('axios'),
        // mongoose =  require('mongoose'),
        fs =        require('fs');
// mongoose.connect('mongodb://localhost:27017/mineraldat');

async function fetchMineralNames() {
    let charsToFetch = 26;
    let mineralNames = [];
    let currentGroupIndex = 0;
    let currentNameIndex = 0;
    for (let i = 0; i < charsToFetch; i++) {
        const response = await axios.get(`https://en.wikipedia.org/w/api.php?action=parse&page=List%20of%20minerals&section=${i + 1}&prop=links&format=json`);
        for (let j = 0; j < response.data.parse.links.length; j++) {
            if (response.data.parse.links[j]['*'].toLowerCase().charAt(0) === String.fromCharCode(97 + i)) {
                if (response.data.parse.links[j]['*'] === 'Kaňkite' || response.data.parse.links[j]['*'] === 'Felsőbányaite' || response.data.parse.links[j]['*'] === 'Joaquinite-(Ce)') {
                    console.log("Skipping unparsable mineral");
                    continue;
                }
                if (currentNameIndex % 50 === 0) {
                    if (currentNameIndex !== 0) {
                        currentGroupIndex++;
                    } 
                    mineralNames[currentGroupIndex] = response.data.parse.links[j]['*'];
                } else {
                    mineralNames[currentGroupIndex] += `|${response.data.parse.links[j]['*']}`;
                }
                currentNameIndex++;
            }
        }    
    }
    console.log(`Mineral Names Fetching completed`);
    return mineralNames;
}

async function fetchMineralData(mineralNames, redirectIteration = 0) {
    var redirects = [];
    var hashRedirects = [];
    let failedMinerals = [];
    let mineralData = {};
    let redirectsCount = 0;
    let currentRedirectIndex = 0;

    for (let i = 0; i < mineralNames.length; i++) {
        const response = await axios.get(`https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&titles=${mineralNames[i]}&rvsection=0`);
        if (response.data.error && response.data.error.code) {
            if (response.data.error.code === 'nosuchsection') {
                console.log(`failed to fetch all mineral data - no such section`);
                return null;
            }
        } else {
            let pageKeys = Object.keys(response.data.query.pages);
            for (let i = 0; i < pageKeys.length; i++) {
                let queryData = response.data.query.pages[pageKeys[i]];
                if (queryData.revisions[0]['*'].toLowerCase().includes('#redirect')) {
                    if (queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1].includes('#')) {
                        hashRedirects.push(queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1]);
                    } else {
                        if (redirectsCount % 50 === 0) {
                            if (redirectsCount !== 0) {
                                currentRedirectIndex++;
                            }
                            redirects[currentRedirectIndex] = queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1];
                        } else {
                            redirects[currentRedirectIndex] += `|${queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1]}`;
                        }
                        redirectsCount++;
                    }
                } else {
                    let tempMineralData = {};
                    let mineralName = queryData.title;
                    let rawData = queryData.revisions[0]['*'];
                    console.log(`${mineralName} parsing begin`);
                    
                    if (rawData.includes(`\n'''`)) {
                        rawData = rawData.substring(0, rawData.indexOf(`\n'''`));
                    }

                    let splitData = rawData.split(/\s\|\s/);
                    
                    splitData.forEach(function(fetchedData) {
                        fetchedData = fetchedData.trim();
                        if (fetchedData.search(/\s\=\s/) !== -1) {
                            let splitKeyVals = fetchedData.split(/\s\=\s/);
                            tempMineralData[splitKeyVals[0].trim()] = splitKeyVals[1].trim();
                        }
                    });

                    if (!tempMineralData['name'] || 
                        (!tempMineralData['name'].toLowerCase().includes(mineralName.toLowerCase()) && !mineralName.toLowerCase().includes(tempMineralData['name'].toLowerCase()))) {
                        failedMinerals.push(queryData.title);
                        console.log(`${mineralName} parsing failed - No name attribute`);
                    } else {
                        mineralData[mineralName] = tempMineralData;
                        console.log(`${mineralName} parsed successully`);
                    }
                }
            }
        }
    }
    if (hashRedirects.length > 0 && redirects.length > 0) {
        redirects = [...redirects, ...hashRedirects];
    } else if (hashRedirects.length > 0) {
        redirects = hashRedirects;
    }
    if (redirects.length > 0 && redirectIteration < 5) {
        console.log(`Processing redirects - Group ${redirectIteration}`);
        let redirectData = await fetchMineralData(redirects, ++redirectIteration);
        mineralData = {...mineralData, ...redirectData.mineralData};
        failedMinerals = [...failedMinerals, ...redirectData.failedMinerals];
    }

    console.log(`Mineral Data fetching complete`);
    return {
        mineralData: mineralData, 
        failedMinerals: failedMinerals
    };
}

async function writeMineralProperties(mineralData) {
    let mineralAttributes = ["name", "class", "HMSymbol", "category", "color", "habit", "twinning", "cleavage", "fracture", "tenacity", "mohs", "mohsLow", "mohsHigh", "luster", "streak", "diaphaneity", "gravity", "gravityLow", "gravityHigh", "refractive", "birefringence", "pleochroism", "dispersion", "solubility", "fluorescence", "twoV", "formula", "molweight", "strunz", "dana", "crystalSystem", "unitCell", "symmetry", "opticalprop", "image"];
    let stream = fs.createWriteStream('minerals.csv');
    await stream.once('open', function(fd) {
        for (let i = 0; i < mineralAttributes.length; i++) {
            stream.write(mineralAttributes[i]);
            if (i < mineralAttributes.length - 1) {
                stream.write('~');
            }
        }
        stream.write('\n');
        let minNames = Object.keys(mineralData);

        for (let i = 0; i < minNames.length; i++) {
            if (!mineralData[minNames[i]]["color"] && mineralData[minNames[i]]["colour"]) {
                mineralData[minNames[i]]["color"] = mineralData[minNames[i]]["colour"];
            }
            if (!mineralData[minNames[i]]["luster"] && mineralData[minNames[i]]["lustre"]) {
                mineralData[minNames[i]]["luster"] = mineralData[minNames[i]]["lustre"];
            }
            if (mineralData[minNames[i]]["2v"]) {
                mineralData[minNames[i]]["twoV"] = mineralData[minNames[i]]["2v"];
            }
            if (mineralData[minNames[i]]["system"]) {
                mineralData[minNames[i]]["crystalSystem"] = mineralData[minNames[i]]["system"];
            }
            if (mineralData[minNames[i]]["unit cell"]) {
                mineralData[minNames[i]]["unitCell"] = mineralData[minNames[i]]["unit cell"];
            }
            if (!mineralData[minNames[i]]["molweight"]) {
                if (mineralData[minNames[i]]["molecular weight"]) {
                    mineralData[minNames[i]]["molweight"] = mineralData[minNames[i]]["molecular weight"];
                } else if (mineralData[minNames[i]]["molarmass"]) {
                    mineralData[minNames[i]]["molweight"] = mineralData[minNames[i]]["molarmass"];
                }
            }
            if (mineralData[minNames[i]]["class"]) {
                mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].replace(/\{\{|\}\}|<small>|<\/small>|<br\s*\/>/gm, '').trim()
                mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].replace('–', '-');
                mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].replace("[[H-M Symbol]]", "[[H-M symbol]]");
                if (mineralData[minNames[i]]["class"].includes("[[H-M symbol]]")) {
                    if (mineralData[minNames[i]]["class"].match(/\(.*/) !== null) {
                        let classData = mineralData[minNames[i]]["class"].match(/\(.*/)[0];
                        if (classData.includes("(same [[H-M symbol]])")) {
                            classData = classData.split("(same [[H-M symbol]])");
                            mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].substring(0, mineralData[minNames[i]]["class"].indexOf("(same [[H-M symbol]])")).trim();
                            mineralData[minNames[i]]["HMSymbol"] = classData[0].match(/(.*)/)[0];
                        } else if (classData.includes("[[H-M symbol]]:")) {
                            classData = classData.split("[[H-M symbol]]:");
                            mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].substring(0, mineralData[minNames[i]]["class"].indexOf("[[H-M symbol]]:")).trim();
                            mineralData[minNames[i]]["HMSymbol"] = classData[1].trim();
                        }
                    } else if (mineralData[minNames[i]]["class"].includes("[[H-M symbol]]:")) {
                        let classData = mineralData[minNames[i]]["class"].split("[[H-M symbol]]:");
                        mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].substring(0, mineralData[minNames[i]]["class"].indexOf("[[H-M symbol]]:")).trim();
                        mineralData[minNames[i]]["HMSymbol"] = classData[1].trim();
                    }
                }
            }
            if (mineralData[minNames[i]]["mohs"]) {
                
                mineralData[minNames[i]]["mohs"] = mineralData[minNames[i]]["mohs"].replace(/<ref[^>|\/]*>.*?<\/ref>|<ref[^>]*\/>|\{\{|\}\}/gm, '');
                mineralData[minNames[i]]["mohs"] = mineralData[minNames[i]]["mohs"].replace(/(&nbsp;|\s)*(-|–|&ndash;|â€“|\\u2013|to)(&nbsp;|\s)*/gm, '-');
                
                if (mineralData[minNames[i]]["mohs"].match(/frac\|(\d)\|1\|2/m) !== null) {
                    mineralData[minNames[i]]["mohs"] = mineralData[minNames[i]]["mohs"].replace(/frac\|\d\|1\|2/gm, `${mineralData[minNames[i]]["mohs"].match(/frac\|(\d)\|1\|2/m)[1]}.5`);
                }
                else if (mineralData[minNames[i]]["mohs"].match(/(\d)frac\|1\|2/m) !== null) {
                    mineralData[minNames[i]]["mohs"] = mineralData[minNames[i]]["mohs"].replace(/\dfrac\|1\|2/gm, `${mineralData[minNames[i]]["mohs"].match(/(\d)frac\|1\|2/m)[1]}.5`);
                }

                if (mineralData[minNames[i]]["mohs"].match(/\<\d\.?\d*/gm) !== null) {
                    mineralData[minNames[i]]["mohsLow"] = mineralData[minNames[i]]["mohs"].match(/\<\d\.?\d*/gm)[0];
                    mineralData[minNames[i]]["mohsHigh"] = mineralData[minNames[i]]["mohs"].match(/\<\d\.?\d*/gm)[0];
                } else if (mineralData[minNames[i]]["mohs"].match(/\d\.?\d*/m) !== null) {
                    let match = mineralData[minNames[i]]["mohs"].match(/(\d\.?\d*)/gm);
                    if (match.length === 1) {
                        mineralData[minNames[i]]["mohsLow"] = match[0];
                        mineralData[minNames[i]]["mohsHigh"] = match[0];
                    }
                    else if (match.length === 2) {
                        mineralData[minNames[i]]["mohsLow"] = String(Math.min(match[0], match[1]));
                        mineralData[minNames[i]]["mohsHigh"] = String(Math.max(match[0], match[1]));
                    }
                    else if (match.length > 2) {
                        mineralData[minNames[i]]["mohsLow"] = String(Math.min(match[0], match[1], match[2]));
                        mineralData[minNames[i]]["mohsHigh"] = String(Math.max(match[0], match[1], match[2]));
                    }      
                }
            }
            if (mineralData[minNames[i]]["crystalSystem"]) {
                mineralData[minNames[i]]["crystalSystem"] = mineralData[minNames[i]]["crystalSystem"].replace(/<br\s*\/>/gm, '');
                if (mineralData[minNames[i]]["crystalSystem"].match(/\[\[.*\]\]/gm) !== null) {
                    let match;
                    if (mineralData[minNames[i]]["crystalSystem"].match(/\[\[.+\|(.+)\]\]\s*(.+)/m) !== null) {
                        match = mineralData[minNames[i]]["crystalSystem"].match(/\[\[.+\|(.+)\]\]\s*(.+)/m);
                    } else if (mineralData[minNames[i]]["crystalSystem"].match(/\[\[.+\|(.+)\]\]/m) !== null) {
                        match = mineralData[minNames[i]]["crystalSystem"].match(/\[\[.+\|(.+)\]\]/m);
                    } else if (mineralData[minNames[i]]["crystalSystem"].match(/\[\[(.+)\]\]\s*(.+)/m) !== null) {
                        match = mineralData[minNames[i]]["crystalSystem"].match(/\[\[(.+)\]\]\s*(.+)/m);
                    } else {
                        match = mineralData[minNames[i]]["crystalSystem"].match(/\[\[(.*)\]\]/m);
                    }
                    if (match !== null && match[1]) {
                        mineralData[minNames[i]]["crystalSystem"] = match[1].trim();
                        if (!mineralData[minNames[i]]["class"] && typeof match[2] === 'string') {
                            mineralData[minNames[i]]["class"] = match[2].trim();
                        }
                    }
                }
            }
            if (mineralData[minNames[i]]["category"]) {
                mineralData[minNames[i]]["category"] = mineralData[minNames[i]]["category"].replace(/\s*and\s*/gm, ', ');
            }
            if (mineralData[minNames[i]]["gravity"]) {
                mineralData[minNames[i]]["gravity"] = mineralData[minNames[i]]["gravity"].replace(/\s*(\–|to|-)\s*/gm, '-');
                mineralData[minNames[i]]["gravity"] = mineralData[minNames[i]]["gravity"].replace(/[a-zA-Z|;]+(?![^\(]*\))/gm, '');
                if (mineralData[minNames[i]]["gravity"].match(/(\d+\.\d*\s*)\-(\d+\.\d*)/m) !== null) {
                    let match = mineralData[minNames[i]]["gravity"].match(/(\d+\.\d*\s*)\-(\d+\.\d*)/m);
                        mineralData[minNames[i]]["gravityLow"] = match[1];
                        mineralData[minNames[i]]["gravityHigh"] = match[2];
                } else if (mineralData[minNames[i]]["gravity"].match(/\d+\.\d*/gm) != null && mineralData[minNames[i]]["gravity"].match(/\+\/\-(\d*\.\d*)(?=[^\(]*\))/m) !== null) {
                    let value = mineralData[minNames[i]]["gravity"].match(/(\d+\.\d*)/m)[1];
                    let variance = mineralData[minNames[i]]["gravity"].match(/\+\/\-(\d*\.\d*)(?=[^\(]*\))/m)[1];
                        mineralData[minNames[i]]["gravityLow"] = String(Math.round((parseFloat(value) + parseFloat(variance)) * 100) / 100);
                        mineralData[minNames[i]]["gravityHigh"] = String(Math.round((parseFloat(value) - parseFloat(variance)) * 100) / 100);
                } else if (mineralData[minNames[i]]["gravity"].match(/\d+\.\d*/gm) != null && mineralData[minNames[i]]["gravity"].match(/\+(\d*\.\d*)\s*,*\s*\-(\d*\.\d*)(?=[^\(]*\))/m) !== null) {
                    let value = mineralData[minNames[i]]["gravity"].match(/(\d+\.\d*)/m)[1];
                    let variance = mineralData[minNames[i]]["gravity"].match(/\+(\d*\.\d*)\s*,*\s*\-(\d*\.\d*)(?=[^\(]*\))/m);
                        mineralData[minNames[i]]["gravityLow"] = String(Math.round((parseFloat(value) + parseFloat(variance[1])) * 100) / 100);
                        mineralData[minNames[i]]["gravityHigh"] = String(Math.round((parseFloat(value) - parseFloat(variance[2])) * 100) / 100);
                } else if (mineralData[minNames[i]]["gravity"].match(/(\d+\.\d*).*?(\d+\.\d*)/m) !== null) {
                    let match = mineralData[minNames[i]]["gravity"].match(/(\d+\.\d*).*?(\d+\.\d*)/m);
                        mineralData[minNames[i]]["gravityLow"] = match[1];
                        mineralData[minNames[i]]["gravityHigh"] = match[2];
                } else if (mineralData[minNames[i]]["gravity"].match(/\d+\.\d*/gm) !== null) {
                        mineralData[minNames[i]]["gravityLow"] = mineralData[minNames[i]]["gravity"].match(/\d+\.\d*/m)[0];
                        mineralData[minNames[i]]["gravityHigh"] = mineralData[minNames[i]]["gravity"].match(/\d+\.\d*/m)[0];
                } else {
                        mineralData[minNames[i]]["gravityLow"] = mineralData[minNames[i]]["gravity"];
                        mineralData[minNames[i]]["gravityHigh"] = mineralData[minNames[i]]["gravity"];
                }                
            }

            for (let j = 0; j < mineralAttributes.length; j++) {
                if (mineralData[minNames[i]][mineralAttributes[j]]) {
                    stream.write(mineralData[minNames[i]][mineralAttributes[j]].replace(/~|'''|<ref[^>|\/]*>.*?<\/ref>|<ref[^>]*\/>|<br\s*?\/>|\r?\n|\r|\{\{|\}\}|\[\[|\]\]/gm, ''));
                } 
                if (j < mineralAttributes.length - 1) {
                    stream.write('~');
                } else if (i < minNames.length - 1) {
                    stream.write('\n');
                }
            }
        }
    });
    console.log(`Completed writing mineral properties to csv`);
}

function catalogUniqueAttributes(mineralData) {
    let mineralAttributes = [];
    let mineralNames = Object.keys(mineralData);

    for (let i = 0; i < mineralNames.length; i++) {
        Object.keys(mineralData[mineralNames[i]]).forEach(function(attribute) {
            if (!mineralAttributes.includes(attribute)) {
                mineralAttributes.push(attribute);
            }
        });
    }
    console.log(`Completed cataloging unique mineral attributes`);
    return mineralAttributes;
}

async function writeAttributes(attributes) {
    let stream = fs.createWriteStream('mineralsAttributes.txt');
    await stream.once('open', function(fd) {
       for (let i = 0; i < attributes.length; i++) {
            stream.write(attributes[i]);
            if (i < attributes.length - 1) {
                stream.write('\n');
            }
        }
    });
    console.log(`Completed writing mineral attributes to txt`);
}

async function writeFailedMinerals(failedMinerals) {
    let stream = fs.createWriteStream('failedMinerals.txt');
    await stream.once('open', function(fd) {
        for (let i = 0; i < failedMinerals.length; i++) {
            stream.write(failedMinerals[i]);
            if (i < failedMinerals.length - 1) {
                stream.write('\n');
            }
        }
    });
    console.log(`Completed writing failed minerals to txt`);
}

async function processMinerals() {
    let mineralNames = [];
    let fetchedData = {};
    let mineralAttributeList = [];

    mineralNames = await fetchMineralNames();
    fetchedData = await fetchMineralData(mineralNames);
    
    mineralAttributeList = catalogUniqueAttributes(fetchedData.mineralData);

    console.log(`Writing mineral properties to csv`);
    await writeMineralProperties(fetchedData.mineralData);

    console.log(`Writing failed minerals to txt`);
    await writeFailedMinerals(fetchedData.failedMinerals);

    console.log(`Cataloging unique attributes`);
    mineralAttributeList = catalogUniqueAttributes(fetchedData.mineralData);

    console.log(`Writing unique mineral attributes to txt`);
    await writeAttributes(mineralAttributeList);
}

processMinerals();


