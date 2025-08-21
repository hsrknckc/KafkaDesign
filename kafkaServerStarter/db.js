const sqlite3 = require("sqlite3").verbose();
const db = new sqlite3.Database("users.db");
const bcrypt = require("bcrypt");

// Kullanıcı tablosu oluştur
db.run(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE,
  password TEXT,
  role TEXT,
  groups TEXT
)`);

// Kullanıcı ekle
function addUser(username, password, role, groups, cb) {
  bcrypt.hash(password, 10, (err, hash) => {
    if (err) return cb(err);
    db.run(
      `INSERT INTO users (username, password, role, groups) VALUES (?, ?, ?, ?)`,
      [username, hash, role, groups],
      cb
    );
  });
}

// Giriş kontrolü
function authenticate(username, password, cb) {
  db.get(
    `SELECT * FROM users WHERE username = ?`,
    [username],
    (err, row) => {
      if (err) return cb(err);
      if (!row) return cb(null, false);

      bcrypt.compare(password, row.password, (err, same) => {
        if (err) return cb(err);
        if (!same) return cb(null, false);
        
        db.run(`UPDATE users SET login_time = ? WHERE username = ?`, [
          new Date().toUTCString(),
          username,
        ]);
        cb(null, { role: row.role, groups: row.groups }); // "admin" veya "user"
      });
    }
  );
}

function closeTime(date, username) {
  db.run(`UPDATE users SET logout_time = ? WHERE username = ?`, [
    date,
    username,
  ]);
}

module.exports = { addUser, authenticate, closeTime };
