const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const port = 8015;

app.use(bodyParser.json({ limit: '10000mb' }));
app.use(bodyParser.urlencoded({ limit: '10000mb', extended: true }));
app.use(cors());

// 数据库配置
const DB_CONFIG = {
    host: 'localhost',
    user: 'root',
    password: 'Guoyanjun123.',
    dateStrings: true 
};

let dbName = `terminal_${Date.now()}`;

// Initialize database
async function initDatabase() {
    // console.log(`Initializing database: ${dbName}...`);
    
    const baseDb = mysql.createConnection({
        host: DB_CONFIG.host,
        user: DB_CONFIG.user,
        password: DB_CONFIG.password
    });

    try {
        await new Promise((resolve, reject) => {
            baseDb.connect(err => err ? reject(`Base connection failed: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            baseDb.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``, err =>
                err ? reject(`Failed to create database: ${err.message}`) : resolve()
            );
        });

        baseDb.end();

        const db = mysql.createConnection({
            ...DB_CONFIG,
            database: dbName
        });

        await new Promise((resolve, reject) => {
            db.connect(err => err ? reject(`Failed to connect to new database: ${err.message}`) : resolve());
        });

        // Create Visit table 
        const createVisitTable = `
            CREATE TABLE IF NOT EXISTS Visit (
                拜访记录编号 VARCHAR(50),
                拜访开始时间 VARCHAR(50),
                拜访结束时间 VARCHAR(50),
                拜访人 VARCHAR(50),
                客户名称 VARCHAR(100),
                客户编码 VARCHAR(50),
                拜访用时 INT,
                INDEX idx_visit_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        // Create Terminal table
        const createTerminalTable = `
            CREATE TABLE IF NOT EXISTS Terminal (
                客户编码 VARCHAR(50),
                所属片区 VARCHAR(100),
                所属大区 VARCHAR(100),
                UNIQUE INDEX idx_terminal_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        await new Promise((resolve, reject) => {
            db.query(createVisitTable, err => err ? reject(`Failed to create Visit table: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            db.query(createTerminalTable, err => err ? reject(`Failed to create Terminal table: ${err.message}`) : resolve());
        });

        const oldDb = app.get('db');
        if (oldDb) {
            try { oldDb.end(); } catch(e) {}
        }

        app.set('db', db);
        // console.log(`Database initialization completed: ${dbName}`);

    } catch (error) {
        console.error('Database initialization failed:', error);
        if (process.uptime() < 5) {
            process.exit(1);
        } else {
            throw error;
        }
    }
}

// Upload Visit records
app.post('/api/audit_visit/store_daily_multi_visits/uploadVisit', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;

    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Visit data provided' });
    }

    const values = records.map(r => [
        r.拜访记录编号 || null,
        r.拜访开始时间 || null,
        r.拜访结束时间 || null,
        r.拜访人 || null,
        r.客户名称 || null,
        r.客户编码 || null,
        typeof r.拜访用时 === 'string' ? parseInt(r.拜访用时) || 0 : (r.拜访用时 || 0)
    ]);

    const sql = 'INSERT INTO Visit (拜访记录编号, 拜访开始时间, 拜访结束时间, 拜访人, 客户名称, 客户编码, 拜访用时) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Visit records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} records imported` });
        }
    });
});

// Upload Terminal records 
app.post('/api/audit_visit/store_daily_multi_visits/uploadTerminal', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;

    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Terminal data provided' });
    }

    const values = records.map(r => [
        r.客户编码 || null,
        r.所属片区 || null,
        r.所属大区 || null
    ]);

    const sql = 'INSERT IGNORE INTO Terminal (客户编码, 所属片区, 所属大区) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} records imported` });
        }
    });
});

// ============ 获取单日多人拜访异常 ============
app.get('/api/audit_visit/store_daily_multi_visits/getDailyMultiVisits', (req, res) => {
    const db = app.get('db');

    let {
        targetDate = '',   // 具体日期 YYYY-MM-DD
        minVisitors = 1,   // 最小访客数，默认1（代表至少2人以上即 >1）
        visitor = '',
        customerName = '',
        customerCode = '',
        area = '',
        region = ''
    } = req.query;

    const threshold = parseInt(minVisitors) || 1;
    let conditions = ['d.visitor_count > ?'];
    let params = [threshold];

    if (targetDate) {
        conditions.push('d.visit_date = ?');
        params.push(targetDate);
    }
    if (visitor) {
        conditions.push('d.visitor_list LIKE ?');
        params.push(`%${visitor}%`);
    }
    if (customerName) {
        conditions.push('d.`客户名称` LIKE ?');
        params.push(`%${customerName}%`);
    }
    if (customerCode) {
        conditions.push('d.`客户编码` LIKE ?');
        params.push(`%${customerCode}%`);
    }
    if (area) {
        conditions.push('t.`所属片区` LIKE ?');
        params.push(`%${area}%`);
    }
    if (region) {
        conditions.push('t.`所属大区` LIKE ?');
        params.push(`%${region}%`);
    }

    const whereClause = conditions.join(' AND ');

    // 动态排序逻辑
    const orderByClause = targetDate ? 
        'ORDER BY d.visitor_count DESC, d.visit_date DESC' : 
        'ORDER BY d.visitor_count DESC, d.visit_date DESC';

    const sql = `
        WITH daily_visit AS (
            SELECT
                \`客户编码\`,
                \`客户名称\`,
                DATE_FORMAT(DATE(REPLACE(\`拜访开始时间\`, '/', '-')), '%Y-%m-%d') AS visit_date,
                GROUP_CONCAT(DISTINCT \`拜访人\` SEPARATOR ', ') AS visitor_list,
                COUNT(DISTINCT \`拜访人\`) AS visitor_count
            FROM Visit
            WHERE \`拜访开始时间\` IS NOT NULL AND \`拜访开始时间\` != ''
            GROUP BY 
                \`客户编码\`, 
                \`客户名称\`, 
                visit_date
        )
        SELECT
            d.visitor_list AS 拜访人列表,
            d.\`客户名称\`,
            d.\`客户编码\`,
            d.visit_date AS 拜访日期,
            d.visitor_count AS 当天拜访人数,
            t.所属片区,
            t.所属大区
        FROM daily_visit d
        LEFT JOIN Terminal t ON d.\`客户编码\` = t.\`客户编码\`
        WHERE ${whereClause}
        ${orderByClause};
    `;

    db.query(sql, params, (err, results) => {
        if (err) {
            console.error('Failed to query daily multi visits:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, data: results });
        }
    });
});

// ============ Manual Cleanup Logic ============
app.post('/api/audit_visit/store_daily_multi_visits/cleanup', async (req, res) => {
    // console.log('Manual database cleanup requested...');
    const db = app.get('db');
    try {
        await new Promise((resolve, reject) => {
            db.query('TRUNCATE TABLE Visit', err => err ? reject(err) : resolve());
        });
        await new Promise((resolve, reject) => {
            db.query('TRUNCATE TABLE Terminal', err => err ? reject(err) : resolve());
        });
        // console.log('Database tables cleared successfully');
        res.json({ success: true, message: `Data has been completely cleared.` });
    } catch (error) {
        console.error('Failed to clear database tables:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

function setupProcessCleanup() {
    async function handleExit() {
        try { 
            const db = app.get('db');
            if (db) db.end();
        } catch (error) {}
        process.exit(0);
    }
    process.on('SIGINT', handleExit);
    process.on('SIGTERM', handleExit);
}

// Initialize and start server
initDatabase().then(() => {
    setupProcessCleanup();
    app.listen(port, () => {
        // console.log('='.repeat(60));
        console.log(`Server running on http://localhost:${port}`);
        // console.log(`Current database: ${dbName}`);
        // console.log('API Route: /api/audit_visit/store_daily_multi_visits');
        // console.log('='.repeat(60));
    });
}).catch(error => {
    console.error('Failed to start server:', error);
    process.exit(1);
});