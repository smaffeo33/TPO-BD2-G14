const neo4j = require('neo4j-driver');


const NEO4J_URI = process.env.NEO4J_URI || 'neo4j://localhost:7687';
const NEO4J_USER = process.env.NEO4J_USER || 'neo4j';
const NEO4J_PASS = process.env.NEO4J_PASS || 'password123';

const driver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASS));

const checkNeo4jConnection = async () => {
    try {
        await driver.verifyConnectivity();
        console.log('Neo4j conectado exitosamente.');
    } catch (error) {
        console.error('Error al conectar a Neo4j:', error);
        process.exit(1);
    }
};


const getNeo4jSession = () => {
    return driver.session({ database: 'neo4j' });
};

module.exports = { driver, checkNeo4jConnection, getNeo4jSession };