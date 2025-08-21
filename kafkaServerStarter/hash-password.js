const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcrypt');

const db = new sqlite3.Database('users.db');

// Kullanıcıları çek
db.all(`SELECT id, username, password FROM users`, async (err, rows) => {
  if (err) {
    console.error("DB error:", err);
    return;
  }

  for (const user of rows) {
    const { id, password } = user;

    // Eğer şifre zaten bcrypt hash ise atla
    // (bcrypt hash'ler genelde $2a$, $2b$ veya $2y$ ile başlar)
    if (password.startsWith("$2")) {
      console.log(`User ${user.username}: already hashed, skipping`);
      continue;
    }

    try {
      const hash = await bcrypt.hash(password, 10);

      db.run(
        `UPDATE users SET password = ? WHERE id = ?`,
        [hash, id],
        (err) => {
          if (err) {
            console.error(`User ${user.username}: update failed`, err);
          } else {
            console.log(`User ${user.username}: password updated`);
          }
        }
      );
    } catch (e) {
      console.error(`User ${user.username}: hash failed`, e);
    }
  }
});
