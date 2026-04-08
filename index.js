const fs = require('fs');
const path = require('path');
const geoip = require('geoip-lite');
const express = require('express');
const app = express();
const server = require('http').createServer(app);
const io = require('socket.io')(server);
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.static(__dirname));
app.use(express.json());

// Data structures
const users = new Map(); // username -> {socketId, avatar, status, joinTime, gifts, level, xp, coins, isLoggedIn, activeBadge}
const userSessions = new Set();
const userProfiles = new Map(); // username -> {bio, receivedGifts, sentGifts, totalMessages, achievements, coins, password, inventory, favoriteGifts, streaks, isModerator, settings, badges, giftPoints, activeBadge}
const onlineStatus = new Map(); // username -> lastSeen
const blockedUsers = new Map(); // username -> Set of blocked usernames
const privateChats = new Map(); // username1-username2 -> messages array
const bannedUsers = new Set(); // Set of banned usernames
const moderators = new Set(['admin', 'moderator', 'avasfge']); // Set of moderator usernames

// Admin credentials
const adminCredentials = {
    'avasfge': 'mihalchik67'
};

// Telegram-style gifts with animations + NFT gifts
const telegramGifts = {
    // Common gifts
    '🎁': { name: 'Подарок', rarity: 'common', value: 10, animation: 'bounce', badge: null },
    '🌹': { name: 'Роза', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '💐': { name: 'Букет', rarity: 'common', value: 20, animation: 'slide', badge: null },
    '🍰': { name: 'Торт', rarity: 'common', value: 25, animation: 'zoom', badge: null },

    // Uncommon gifts
    '🏆': { name: 'Кубок', rarity: 'uncommon', value: 35, animation: 'bounce', badge: 'winner' },
    '💍': { name: 'Кольцо', rarity: 'uncommon', value: 40, animation: 'sparkle', badge: null },
    '🎭': { name: 'Маска', rarity: 'uncommon', value: 45, animation: 'fade', badge: null },

    // Rare gifts
    '💎': { name: 'Бриллиант', rarity: 'rare', value: 50, animation: 'sparkle', badge: 'vip' },
    '👑': { name: 'Корона', rarity: 'rare', value: 75, animation: 'glow', badge: 'royal' },
    '🔮': { name: 'Магический шар', rarity: 'rare', value: 80, animation: 'rainbow', badge: null },

    // Legendary gifts
    '🦄': { name: 'Единорог', rarity: 'legendary', value: 100, animation: 'rainbow', badge: 'legend' },
    '🔥': { name: 'Огонь любви', rarity: 'legendary', value: 150, animation: 'fire', badge: 'passionate' },
    '🎯': { name: 'Цель', rarity: 'legendary', value: 120, animation: 'zoom', badge: 'focused' },

    // Mythic gifts
    '⭐': { name: 'Звезда', rarity: 'mythic', value: 200, animation: 'star', badge: 'superstar' },
    '💫': { name: 'Комета', rarity: 'mythic', value: 300, animation: 'comet', badge: 'cosmic' },
    '🌟': { name: 'Сияющая звезда', rarity: 'mythic', value: 350, animation: 'sparkle', badge: 'divine' },

    // NFT Collection gifts
    '🐉': { name: 'Дракон NFT', rarity: 'nft', value: 500, animation: 'fire', badge: 'dragon_lord' },
    '🎨': { name: 'Арт NFT', rarity: 'nft', value: 400, animation: 'rainbow', badge: 'artist' },
    '🚀': { name: 'Ракета NFT', rarity: 'nft', value: 600, animation: 'zoom', badge: 'astronaut' },
    '⚡': { name: 'Молния NFT', rarity: 'nft', value: 700, animation: 'sparkle', badge: 'lightning' },
    '🌈': { name: 'Радуга NFT', rarity: 'nft', value: 800, animation: 'rainbow', badge: 'rainbow_master' },
    '🎪': { name: 'Цирк NFT', rarity: 'nft', value: 550, animation: 'bounce', badge: 'entertainer' },

    // Ultra Rare NFT
    '👹': { name: 'Демон NFT', rarity: 'ultra_nft', value: 1000, animation: 'fire', badge: 'demon_king' },
    '🎌': { name: 'Флаг NFT', rarity: 'ultra_nft', value: 900, animation: 'glow', badge: 'commander' },
    '🗿': { name: 'Статуя NFT', rarity: 'ultra_nft', value: 1200, animation: 'zoom', badge: 'ancient' },

    // Новые эксклюзивные подарки
    '✨': { name: 'Волшебная палочка', rarity: 'mythic', value: 380, animation: 'sparkle', badge: 'magic_user' },
    '🎀': { name: 'Лента', rarity: 'common', value: 25, animation: 'fade', badge: null },
    '🎈': { name: 'Воздушный шар', rarity: 'uncommon', value: 30, animation: 'bounce', badge: null },
    '🎉': { name: 'Праздничный салют', rarity: 'rare', value: 60, animation: 'explosion', badge: 'celebrator' },
    '🎶': { name: 'Нота', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🎮': { name: 'Геймпад', rarity: 'uncommon', value: 40, animation: 'zoom', badge: 'gamer' },
    '🍕': { name: 'Пицца', rarity: 'common', value: 20, animation: 'slide', badge: null },
    '🍔': { name: 'Бургер', rarity: 'common', value: 22, animation: 'slide', badge: null },
    '🍟': { name: 'Картошка фри', rarity: 'common', value: 18, animation: 'slide', badge: null },
    '🍦': { name: 'Мороженое', rarity: 'common', value: 15, animation: 'zoom', badge: null },
    '🍬': { name: 'Конфета', rarity: 'common', value: 12, animation: 'bounce', badge: null },
    '🍭': { name: 'Леденец', rarity: 'common', value: 13, animation: 'bounce', badge: null },
    '🍫': { name: 'Шоколад', rarity: 'common', value: 25, animation: 'fade', badge: null },
    '🍓': { name: 'Клубника', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🍎': { name: 'Яблоко', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🍊': { name: 'Апельсин', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🍇': { name: 'Виноград', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🍍': { name: 'Ананас', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🥑': { name: 'Авокадо', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🥦': { name: 'Брокколи', rarity: 'common', value: 15, animation: 'fade', badge: null },
    '🥬': { name: 'Салат', rarity: 'common', value: 15, animation: 'fade', badge: null }
};

// Use Telegram-style gifts
const giftTypes = telegramGifts;

const logFilePath = 'chat.log';
const dataDir = './data';
const profilesFile = path.join(dataDir, 'profiles.json');
const privateChatsFile = path.join(dataDir, 'private_chats.json');
const bannedUsersFile = path.join(dataDir, 'banned_users.json');

// Ensure data directory exists
if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
}

// Load saved data
function loadData() {
    try {
        if (fs.existsSync(profilesFile)) {
            const data = JSON.parse(fs.readFileSync(profilesFile, 'utf8'));
            Object.entries(data).forEach(([username, profile]) => {
                userProfiles.set(username, profile);
            });
        }

        if (fs.existsSync(privateChatsFile)) {
            const data = JSON.parse(fs.readFileSync(privateChatsFile, 'utf8'));
            Object.entries(data).forEach(([chatId, messages]) => {
                privateChats.set(chatId, messages);
            });
        }

        if (fs.existsSync(bannedUsersFile)) {
            const data = JSON.parse(fs.readFileSync(bannedUsersFile, 'utf8'));
            data.forEach(username => bannedUsers.add(username));
        }
    } catch (error) {
        console.error('Error loading data:', error);
    }
}

// Save data
function saveData() {
    try {
        const profilesData = Object.fromEntries(userProfiles);
        fs.writeFileSync(profilesFile, JSON.stringify(profilesData, null, 2));

        const privateChatsData = Object.fromEntries(privateChats);
        fs.writeFileSync(privateChatsFile, JSON.stringify(privateChatsData, null, 2));

        const bannedUsersData = Array.from(bannedUsers);
        fs.writeFileSync(bannedUsersFile, JSON.stringify(bannedUsersData, null, 2));
    } catch (error) {
        console.error('Error saving data:', error);
    }
}

// Hash password (simple implementation - in production use bcrypt)
function hashPassword(password) {
    return Buffer.from(password).toString('base64');
}

// Verify password
function verifyPassword(password, hash) {
    return hashPassword(password) === hash;
}

// Check if user is admin with special credentials
function isAdminUser(username, password) {
    return adminCredentials[username] === password;
}

// Initialize profile for new user
function initializeProfile(username, password = null) {
    if (!userProfiles.has(username)) {
        userProfiles.set(username, {
            bio: 'Привет! Я новичок в чате.',
            receivedGifts: [],
            sentGifts: [],
            totalMessages: 0,
            achievements: [],
            joinDate: new Date().toISOString(),
            level: 1,
            xp: 0,
            avatar: generateAvatar(username),
            status: 'В сети',
            coins: 100,
            giftPoints: 0,
            favoriteGifts: [],
            streaks: { daily: 0, messaging: 0 },
            password: password ? hashPassword(password) : null,
            isModerator: moderators.has(username.toLowerCase()),
            settings: {
                allowPrivateMessages: true,
                showOnlineStatus: true,
                soundNotifications: true
            },
            badges: [],
            activeBadge: null,
            inventory: {
                gifts: {},
                boosters: {},
                themes: [],
                effects: []
            }
        });
    }
}

// Generate avatar color based on username
function generateAvatar(username) {
    const colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
        '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9'
    ];
    let hash = 0;
    for (let i = 0; i < username.length; i++) {
        hash = username.charCodeAt(i) + ((hash << 5) - hash);
    }
    return colors[Math.abs(hash) % colors.length];
}

// Award XP and coins
function awardXP(username, amount) {
    const profile = userProfiles.get(username);
    if (profile) {
        profile.xp += amount;
        const newLevel = Math.floor(profile.xp / 100) + 1;

        const coinAmount = Math.floor(amount / 10);
        profile.coins += coinAmount;

        if (users.has(username)) {
            users.get(username).coins = profile.coins;
        }

        if (newLevel > profile.level) {
            profile.level = newLevel;
            profile.coins += newLevel * 10;
            if (users.has(username)) {
                users.get(username).coins = profile.coins;
            }
            return true;
        }
    }
    return false;
}

// Get user rank based on level and activity
function getUserRank(level, totalMessages) {
    if (level >= 20 || totalMessages >= 500) return 'mythic';
    if (level >= 15 || totalMessages >= 250) return 'legend';
    if (level >= 10 || totalMessages >= 100) return 'veteran';
    if (level >= 5 || totalMessages >= 50) return 'active';
    return 'newbie';
}

// Check achievements
function checkAchievements(username) {
    const profile = userProfiles.get(username);
    if (!profile) return [];

    const newAchievements = [];
    const achievements = [
        { id: 'first_steps', condition: profile.totalMessages >= 10, name: 'Первые шаги (10 сообщений)' },
        { id: 'popular', condition: profile.receivedGifts.length >= 5, name: 'Популярный (5 подарков получено)' },
        { id: 'generous', condition: profile.sentGifts.length >= 10, name: 'Щедрый (10 подарков отправлено)' },
        { id: 'gift_collector', condition: profile.giftPoints >= 100, name: 'Коллекционер подарков (100 очков подарков)' }
    ];

    achievements.forEach(achievement => {
        if (achievement.condition && !profile.achievements.includes(achievement.id)) {
            newAchievements.push(achievement.id);
            profile.achievements.push(achievement.id);
        }
    });

    return newAchievements;
}

// Get private chat ID
function getChatId(user1, user2) {
    return [user1, user2].sort().join('-');
}

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/chat.html');
});

// Authentication endpoints
app.post('/api/register', (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.json({ success: false, message: 'Имя пользователя и пароль обязательны' });
    }

    if (userProfiles.has(username)) {
        return res.json({ success: false, message: 'Пользователь уже существует' });
    }

    if (bannedUsers.has(username)) {
        return res.json({ success: false, message: 'Пользователь заблокирован' });
    }

    initializeProfile(username, password);
    saveData();

    res.json({ success: true, message: 'Регистрация успешна' });
});

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.json({ success: false, message: 'Имя пользователя и пароль обязательны' });
    }

    if (bannedUsers.has(username)) {
        return res.json({ success: false, message: 'Пользователь заблокирован' });
    }

    // Check admin credentials first
    if (isAdminUser(username, password)) {
        if (!userProfiles.has(username)) {
            initializeProfile(username, password);
            userProfiles.get(username).isModerator = true;
            saveData();
        }
        return res.json({ success: true, message: 'Вход администратора выполнен успешно' });
    }

    const profile = userProfiles.get(username);
    if (!profile) {
        return res.json({ success: false, message: 'Пользователь не найден' });
    }

    if (!profile.password) {
        // Old user without password - set password
        profile.password = hashPassword(password);
        saveData();
        return res.json({ success: true, message: 'Пароль установлен для существующего пользователя' });
    }

    if (!verifyPassword(password, profile.password)) {
        return res.json({ success: false, message: 'Неверный пароль' });
    }

    res.json({ success: true, message: 'Вход выполнен успешно' });
});

// Change username endpoint
app.post('/api/change-username', (req, res) => {
    const { oldUsername, newUsername, password } = req.body;

    if (!oldUsername || !newUsername || !password) {
        return res.json({ success: false, message: 'Все поля обязательны' });
    }

    if (newUsername.length < 3 || newUsername.length > 20) {
        return res.json({ success: false, message: 'Имя должно быть от 3 до 20 символов' });
    }

    if (userProfiles.has(newUsername)) {
        return res.json({ success: false, message: 'Имя пользователя уже занято' });
    }

    const profile = userProfiles.get(oldUsername);
    if (!profile) {
        return res.json({ success: false, message: 'Пользователь не найден' });
    }

    if (!verifyPassword(password, profile.password)) {
        return res.json({ success: false, message: 'Неверный пароль' });
    }

    // Transfer profile to new username
    userProfiles.set(newUsername, profile);
    userProfiles.delete(oldUsername);

    // Update active user if online
    if (users.has(oldUsername)) {
        const userData = users.get(oldUsername);
        users.set(newUsername, userData);
        users.delete(oldUsername);
    }

    saveData();
    res.json({ success: true, message: 'Имя пользователя изменено успешно' });
});

// Load data on startup
loadData();

// Auto-save every 5 minutes
setInterval(saveData, 5 * 60 * 1000);

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server is running on port: ${PORT}`);
    logToFile(`\n=== Server started on port: ${PORT} ===`);
});

function logToFile(message) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;
    fs.appendFile(logFilePath, logMessage + '\n', err => {
        if (err) console.error('Error writing to log file:', err);
    });
}

function formatLog(action, details) {
    const separator = '='.repeat(60);
    return `${separator}\nAction: ${action}\n${details}\n${separator}`;
}

io.on('connection', (socket) => {
    const userIP = socket.handshake.address || socket.request.connection.remoteAddress;
    const geo = geoip.lookup(userIP);

    console.log(`New socket connection: ${socket.id} from IP: ${userIP}`);

    socket.on('authenticated user', (username, userAgent = 'Unknown') => {
        console.log(`Authentication attempt for user: ${username}`);

        if (bannedUsers.has(username)) {
            console.log(`Banned user ${username} attempted to connect`);
            socket.emit('user banned', 'Вы заблокированы');
            return;
        }

        const profile = userProfiles.get(username);
        if (!profile) {
            console.log(`Profile not found for user: ${username}`);
            socket.emit('auth error', 'Профиль не найден. Необходимо зарегистрироваться.');
            return;
        }

        // Disconnect any existing connection for this user
        const existingUser = users.get(username);
        if (existingUser && existingUser.socketId !== socket.id) {
            console.log(`Disconnecting existing session for user: ${username}`);
            const existingSocket = io.sockets.sockets.get(existingUser.socketId);
            if (existingSocket) {
                existingSocket.emit('force disconnect', 'Новое подключение с другого устройства');
                existingSocket.disconnect();
            }
        }

        const isNewUser = !userSessions.has(username);

        // Remove old username if exists for this socket
        if (socket.username && socket.username !== username && users.has(socket.username)) {
            users.delete(socket.username);
            userSessions.delete(socket.username);
        }

        socket.username = username;

        // Обновляем данные пользователя
        const userData = {
            socketId: socket.id,
            avatar: profile.avatar,
            status: 'В сети',
            joinTime: existingUser ? existingUser.joinTime : new Date(),
            level: profile.level,
            xp: profile.xp,
            coins: profile.coins || 100,
            isLoggedIn: true,
            isModerator: profile.isModerator || false,
            lastActivity: new Date(),
            typing: false,
            activeBadge: profile.activeBadge
        };

        users.set(username, userData);
        userSessions.add(username);
        onlineStatus.set(username, new Date());

        if (isNewUser) {
            io.emit('send message', {
                message: `${socket.username} присоединился к чату`,
                user: "Система",
                type: 'system'
            });
        }

        // Send user data
        socket.emit('profile data', profile);
        socket.emit('users update', getUsersData());
        socket.emit('gift types', giftTypes);

        // Send user their private chats
        socket.emit('private chats', getPrivateChatsForUser(username));

        // Send updated profile with all gifts data
        socket.emit('user profile update', {
            username: username,
            receivedGifts: profile.receivedGifts || [],
            sentGifts: profile.sentGifts || [],
            badges: profile.badges || [],
            giftPoints: profile.giftPoints || 0,
            activeBadge: profile.activeBadge
        });

        // Send online user stats
        const onlineStats = {
            totalUsers: userProfiles.size,
            onlineUsers: users.size,
            newUsersToday: Array.from(userProfiles.values()).filter(p =>
                new Date(p.joinDate).toDateString() === new Date().toDateString()
            ).length
        };
        socket.emit('online stats', onlineStats);

        // Broadcast user list update to all users
        io.emit('users update', getUsersData());

        // Подтверждение успешной авторизации
        socket.emit('auth success', {
            username: username,
            profile: profile
        });

        const location = geo ? `${geo.city || 'Unknown City'}, ${geo.country}` : 'Unknown location';
        const details = `Username: ${socket.username}\nSession ID: ${socket.id}\nIP: ${userIP}\nLocation: ${location}\nDevice: ${userAgent}\nOnline Users: ${users.size}`;
        const logMessage = formatLog(isNewUser ? 'New User Connected' : 'User Reconnected', details);
        console.log(logMessage);
        logToFile(logMessage);
    });

    socket.on('typing start', () => {
        if (socket.username && users.has(socket.username)) {
            const userData = users.get(socket.username);
            userData.typing = true;
            users.set(socket.username, userData);
            socket.broadcast.emit('user typing', socket.username);
        }
    });

    socket.on('typing stop', () => {
        if (socket.username && users.has(socket.username)) {
            const userData = users.get(socket.username);
            userData.typing = false;
            users.set(socket.username, userData);
            socket.broadcast.emit('user stopped typing', socket.username);
        }
    });

    socket.on('new message', (msg) => {
        console.log(`Message from ${socket.username}: ${msg.substring(0, 50)}...`);

        // Проверка наличия имени пользователя
        if (!socket.username) {
            console.log('Message rejected: user not authenticated');
            socket.emit('auth error', 'Пользователь не авторизован');
            return;
        }

        // Проверка профиля пользователя
        const profile = userProfiles.get(socket.username);
        if (!profile) {
            console.log(`Message rejected: profile not found for ${socket.username}`);
            socket.emit('auth error', 'Профиль пользователя не найден');
            return;
        }

        // Проверка что пользователь не заблокирован
        if (bannedUsers.has(socket.username)) {
            console.log(`Message rejected: user ${socket.username} is banned`);
            socket.emit('user banned', 'Вы заблокированы');
            return;
        }

        // Убедимся что пользователь в списке активных пользователей
        if (!users.has(socket.username)) {
            console.log(`Re-adding user ${socket.username} to active users list`);
            const userData = {
                socketId: socket.id,
                avatar: profile.avatar,
                status: 'В сети',
                joinTime: new Date(),
                level: profile.level,
                xp: profile.xp,
                coins: profile.coins || 100,
                isLoggedIn: true,
                isModerator: profile.isModerator || false,
                lastActivity: new Date(),
                typing: false,
                activeBadge: profile.activeBadge
            };
            users.set(socket.username, userData);
            io.emit('users update', getUsersData());
        } else {
            // Обновляем активность и socket ID
            const userData = users.get(socket.username);
            userData.socketId = socket.id;
            userData.lastActivity = new Date();
            userData.typing = false;
            users.set(socket.username, userData);
        }

        // Stop typing indicator
        socket.broadcast.emit('user stopped typing', socket.username);

        // Обработка сообщения
        profile.totalMessages++;
        const leveledUp = awardXP(socket.username, 5);
        const achievements = checkAchievements(socket.username);

        if (leveledUp) {
            io.emit('send message', {
                message: `${socket.username} достиг ${profile.level} уровня! 🎉`,
                user: "Система",
                type: 'level_up'
            });

            // Real-time profile update
            io.emit('user profile update', {
                username: socket.username,
                level: profile.level,
                xp: profile.xp,
                coins: profile.coins
            });
        }

        achievements.forEach(achievement => {
            const achievementNames = {
                'first_steps': 'Первые шаги (10 сообщений)',
                'popular': 'Популярный (5 подарков получено)',
                'generous': 'Щедрый (10 подарков отправлено)',
                'gift_collector': 'Коллекционер подарков (100 очков подарков)'
            };
            io.emit('send message', {
                message: `${socket.username} получил достижение: ${achievementNames[achievement]} 🏆`,
                user: "Система",
                type: 'achievement'
            });
        });

        const messageData = {
            message: msg,
            user: socket.username,
            timestamp: new Date(),
            type: 'message'
        };
        io.emit('send message', messageData);
        console.log(`Message sent successfully from ${socket.username}`);
        saveData();
    });

    socket.on('send private message', (data) => {
        const { recipient, message } = data;

        if (!socket.username || !users.has(socket.username)) {
            socket.emit('auth error', 'Необходима авторизация');
            return;
        }

        const recipientUser = users.get(recipient);
        if (!recipientUser) {
            socket.emit('error', 'Получатель не в сети');
            return;
        }

        const chatId = getChatId(socket.username, recipient);
        if (!privateChats.has(chatId)) {
            privateChats.set(chatId, []);
        }

        const messageData = {
            from: socket.username,
            to: recipient,
            message,
            timestamp: new Date(),
            read: false
        };

        privateChats.get(chatId).push(messageData);

        // Send to both users
        socket.emit('private message received', messageData);
        io.to(recipientUser.socketId).emit('private message received', messageData);

        saveData();
    });

    socket.on('send gift', (data) => {
        console.log(`Gift send attempt by ${socket.username}:`, data);

        const { gift, recipient } = data;

        if (!socket.username || !users.has(socket.username)) {
            console.log('Gift send failed: user not authenticated');
            socket.emit('error', 'Необходима авторизация для отправки подарков');
            return;
        }

        const senderProfile = userProfiles.get(socket.username);
        if (!senderProfile) {
            console.log('Gift send failed: sender profile not found');
            socket.emit('error', 'Профиль отправителя не найден');
            return;
        }

        const giftInfo = giftTypes[gift];
        if (!giftInfo) {
            console.log('Gift send failed: invalid gift type', gift);
            socket.emit('error', 'Неверный тип подарка');
            return;
        }

        // Initialize gift arrays if not exist
        if (!senderProfile.sentGifts) senderProfile.sentGifts = [];
        if (!senderProfile.badges) senderProfile.badges = [];

        console.log(`Sending gift ${gift} (${giftInfo.name}) from ${socket.username} to ${recipient}`);

        if (recipient === 'all') {
            const onlineUsers = Array.from(users.keys()).filter(u => u !== socket.username);
            console.log(`Sending to all users: ${onlineUsers.length} recipients`);

            onlineUsers.forEach(user => {
                const recipientProfile = userProfiles.get(user);
                if (recipientProfile) {
                    // Initialize arrays if needed
                    if (!recipientProfile.receivedGifts) recipientProfile.receivedGifts = [];
                    if (!recipientProfile.favoriteGifts) recipientProfile.favoriteGifts = [];
                    if (!recipientProfile.badges) recipientProfile.badges = [];

                    const giftData = {
                        gift,
                        from: socket.username,
                        to: user,
                        timestamp: new Date(),
                        rarity: giftInfo.rarity,
                        value: giftInfo.value,
                        animation: giftInfo.animation,
                        badge: giftInfo.badge
                    };

                    recipientProfile.receivedGifts.push(giftData);
                    recipientProfile.giftPoints = (recipientProfile.giftPoints || 0) + giftInfo.value;

                    // Add badge if gift has one
                    if (giftInfo.badge && !recipientProfile.badges.includes(giftInfo.badge)) {
                        recipientProfile.badges.push(giftInfo.badge);
                        console.log(`Added badge ${giftInfo.badge} to ${user}`);
                    }

                    // Update favorite gifts
                    const giftCount = recipientProfile.favoriteGifts.find(g => g && g.gift === gift);
                    if (giftCount) {
                        giftCount.count++;
                    } else {
                        recipientProfile.favoriteGifts.push({ gift, count: 1 });
                    }

                    const recipientUser = users.get(user);
                    if (recipientUser) {
                        io.to(recipientUser.socketId).emit('gift animation', {
                            gift,
                            animation: giftInfo.animation,
                            from: socket.username,
                            giftInfo
                        });

                        io.to(recipientUser.socketId).emit('gift received', {
                            gift,
                            giftInfo,
                            from: socket.username,
                            timestamp: new Date(),
                            badge: giftInfo.badge
                        });
                    }
                }
            });

            // Add to sender's sent gifts for each recipient
            onlineUsers.forEach(user => {
                senderProfile.sentGifts.push({
                    gift,
                    from: socket.username,
                    to: user,
                    timestamp: new Date(),
                    rarity: giftInfo.rarity,
                    value: giftInfo.value,
                    animation: giftInfo.animation,
                    badge: giftInfo.badge
                });
            });

            io.emit('send message', {
                message: `${socket.username} отправил ${giftInfo.name} ${gift} всем участникам чата! 🎁✨`,
                user: "Система",
                type: 'gift'
            });

            console.log(`Gift sent to all ${onlineUsers.length} users successfully`);

        } else {
            // Single recipient
            const recipientProfile = userProfiles.get(recipient);

            if (!recipientProfile) {
                console.log('Gift send failed: recipient profile not found');
                socket.emit('error', 'Получатель не найден');
                return;
            }

            // Initialize arrays if needed
            if (!recipientProfile.receivedGifts) recipientProfile.receivedGifts = [];
            if (!recipientProfile.favoriteGifts) recipientProfile.favoriteGifts = [];
            if (!recipientProfile.badges) recipientProfile.badges = [];

            const giftData = {
                gift,
                from: socket.username,
                to: recipient,
                timestamp: new Date(),
                rarity: giftInfo.rarity,
                value: giftInfo.value,
                animation: giftInfo.animation,
                badge: giftInfo.badge
            };

            senderProfile.sentGifts.push(giftData);
            recipientProfile.receivedGifts.push(giftData);
            recipientProfile.giftPoints = (recipientProfile.giftPoints || 0) + giftInfo.value;

            // Add badge if gift has one
            if (giftInfo.badge && !recipientProfile.badges.includes(giftInfo.badge)) {
                recipientProfile.badges.push(giftInfo.badge);
                console.log(`Added badge ${giftInfo.badge} to ${recipient}`);
            }

            // Update favorite gifts
            const giftCount = recipientProfile.favoriteGifts.find(g => g && g.gift === gift);
            if (giftCount) {
                giftCount.count++;
            } else {
                recipientProfile.favoriteGifts.push({ gift, count: 1 });
            }

            io.emit('send message', {
                message: `${socket.username} отправил ${giftInfo.name} ${gift} для ${recipient} 🎁${giftInfo.badge ? ' (+badge)' : ''}`,
                user: "Система",
                type: 'gift'
            });

            const recipientUser = users.get(recipient);
            if (recipientUser) {
                io.to(recipientUser.socketId).emit('gift animation', {
                    gift,
                    animation: giftInfo.animation,
                    from: socket.username,
                    giftInfo
                });

                io.to(recipientUser.socketId).emit('gift received', {
                    gift,
                    giftInfo,
                    from: socket.username,
                    timestamp: new Date(),
                    badge: giftInfo.badge
                });

                // Real-time profile update
                io.to(recipientUser.socketId).emit('profile gift update', {
                    receivedGifts: recipientProfile.receivedGifts,
                    giftPoints: recipientProfile.giftPoints,
                    badges: recipientProfile.badges
                });
            }

            console.log(`Gift sent to ${recipient} successfully`);
        }

        // Award XP to sender
        awardXP(socket.username, Math.floor(giftInfo.value / 5));
        const newAchievements = checkAchievements(socket.username);

        if (newAchievements.length > 0) {
            console.log(`New achievements for ${socket.username}:`, newAchievements);
        }

        // Send confirmation to sender
        socket.emit('gift sent confirmation', {
            gift: gift,
            giftInfo: giftInfo,
            recipient: recipient,
            message: `Подарок ${giftInfo.name} ${gift} успешно отправлен!`
        });

        // Update user list to show new badges
        io.emit('users update', getUsersData());

        saveData();
        console.log('Gift transaction completed and data saved');
    });

    // Change username
    socket.on('change username', (data) => {
        const { newUsername } = data;

        if (!socket.username || !users.has(socket.username)) {
            socket.emit('error', 'Необходима авторизация');
            return;
        }

        if (!newUsername || newUsername.length < 3 || newUsername.length > 20) {
            socket.emit('error', 'Имя должно быть от 3 до 20 символов');
            return;
        }

        if (userProfiles.has(newUsername)) {
            socket.emit('error', 'Имя пользователя уже занято');
            return;
        }

        const oldUsername = socket.username;

        // Update profile
        const profile = userProfiles.get(oldUsername);
        if (profile) {
            userProfiles.set(newUsername, profile);
            userProfiles.delete(oldUsername);
        }

        // Update user data
        const userData = users.get(oldUsername);
        if (userData) {
            users.set(newUsername, userData);
            users.delete(oldUsername);
        }

        // Update socket
        socket.username = newUsername;

        // Notify all users
        io.emit('send message', {
            message: `${oldUsername} сменил имя на ${newUsername}`,
            user: "Система",
            type: 'system'
        });

        // Update user list
        io.emit('users update', getUsersData());

        // Confirm to user
        socket.emit('username changed', {
            oldUsername,
            newUsername
        });

        saveData();
    });

    // Admin panel actions
    socket.on('admin get users', () => {
        if (!socket.username || !users.has(socket.username)) return;

        const moderatorProfile = userProfiles.get(socket.username);
        if (!moderatorProfile || !moderatorProfile.isModerator) {
            socket.emit('error', 'Недостаточно прав');
            return;
        }

        const allUsers = Array.from(userProfiles.entries()).map(([username, profile]) => ({
            username,
            level: profile.level,
            totalMessages: profile.totalMessages,
            joinDate: profile.joinDate,
            isOnline: users.has(username),
            isBanned: bannedUsers.has(username),
            isModerator: profile.isModerator
        }));

        socket.emit('admin users list', allUsers);
    });

    socket.on('admin get stats', () => {
        if (!socket.username || !users.has(socket.username)) return;

        const moderatorProfile = userProfiles.get(socket.username);
        if (!moderatorProfile || !moderatorProfile.isModerator) {
            socket.emit('error', 'Недостаточно прав');
            return;
        }

        const stats = {
            totalUsers: userProfiles.size,
            onlineUsers: users.size,
            bannedUsers: bannedUsers.size,
            totalMessages: Array.from(userProfiles.values()).reduce((sum, p) => sum + p.totalMessages, 0),
            totalGifts: Array.from(userProfiles.values()).reduce((sum, p) => sum + (p.sentGifts?.length || 0), 0)
        };

        socket.emit('admin stats', stats);
    });

    socket.on('admin clear messages', () => {
        if (!socket.username || !users.has(socket.username)) return;

        const moderatorProfile = userProfiles.get(socket.username);
        if (!moderatorProfile || !moderatorProfile.isModerator) {
            socket.emit('error', 'Недостаточно прав');
            return;
        }

        io.emit('clear messages');

        io.emit('send message', {
            message: `Модератор ${socket.username} очистил чат`,
            user: "Система",
            type: 'system'
        });
    });

    // Moderation commands
    socket.on('ban user', (username) => {
        if (!socket.username || !users.has(socket.username)) return;

        const moderatorProfile = userProfiles.get(socket.username);
        if (!moderatorProfile || !moderatorProfile.isModerator) {
            socket.emit('error', 'Недостаточно прав');
            return;
        }

        bannedUsers.add(username);

        // Disconnect banned user
        const bannedUser = users.get(username);
        if (bannedUser) {
            io.to(bannedUser.socketId).emit('user banned', 'Вы были заблокированы модератором');
            users.delete(username);
        }

        io.emit('send message', {
            message: `Пользователь ${username} был заблокирован`,
            user: "Система",
            type: 'system'
        });

        saveData();
    });

    socket.on('unban user', (username) => {
        if (!socket.username || !users.has(socket.username)) return;

        const moderatorProfile = userProfiles.get(socket.username);
        if (!moderatorProfile || !moderatorProfile.isModerator) {
            socket.emit('error', 'Недостаточно прав');
            return;
        }

        bannedUsers.delete(username);

        io.emit('send message', {
            message: `Пользователь ${username} был разблокирован`,
            user: "Система",
            type: 'system'
        });

        saveData();
    });

    // Get user profile
    socket.on('get user profile', (username) => {
        const profile = userProfiles.get(username);
        const userData = users.get(username);

        if (profile) {
            socket.emit('user profile data', {
                username,
                ...profile,
                isOnline: !!userData,
                lastSeen: onlineStatus.get(username) || profile.joinDate,
                receivedGifts: profile.receivedGifts || [],
                sentGifts: profile.sentGifts || [],
                favoriteGifts: profile.favoriteGifts || [],
                badges: profile.badges || [],
                giftPoints: profile.giftPoints || 0,
                activeBadge: profile.activeBadge
            });
        }
    });

    // Gift shop system - buy gifts with coins
    socket.on('buy gift', (data) => {
        const { giftType, quantity = 1 } = data;

        if (!socket.username || !users.has(socket.username)) {
            socket.emit('error', 'Необходима авторизация');
            return;
        }

        const profile = userProfiles.get(socket.username);
        if (!profile) {
            socket.emit('error', 'Профиль не найден');
            return;
        }

        const giftInfo = giftTypes[giftType];
        if (!giftInfo) {
            socket.emit('error', 'Неверный тип подарка');
            return;
        }

        // Calculate cost (gifts cost coins based on their value)
        const giftCost = Math.max(10, Math.floor(giftInfo.value / 2)); // Minimum 10 coins per gift
        const totalCost = giftCost * quantity;
        
        if (profile.coins < totalCost) {
            socket.emit('error', `Недостаточно монет. Нужно: ${totalCost}, у вас: ${profile.coins}`);
            return;
        }

        // Initialize inventory if needed
        if (!profile.inventory) {
            profile.inventory = {
                gifts: {},
                boosters: {},
                themes: [],
                effects: []
            };
        }

        // Add gift to inventory
        if (!profile.inventory.gifts[giftType]) {
            profile.inventory.gifts[giftType] = 0;
        }
        profile.inventory.gifts[giftType] += quantity;

        // Deduct coins
        profile.coins -= totalCost;

        // Update user coins in active users
        if (users.has(socket.username)) {
            users.get(socket.username).coins = profile.coins;
        }

        socket.emit('gift purchased', {
            gift: giftType,
            quantity: quantity,
            newCoins: profile.coins,
            inventory: profile.inventory.gifts,
            cost: totalCost
        });

        io.emit('send message', {
            message: `${socket.username} купил ${quantity}x ${giftInfo.name} ${giftType} за ${totalCost} монет! 💰`,
            user: "Система",
            type: 'system'
        });

        saveData();
    });

    // Send gift from inventory
    socket.on('send gift from inventory', (data) => {
        const { gift, recipient } = data;

        if (!socket.username || !users.has(socket.username)) {
            socket.emit('error', 'Необходима авторизация');
            return;
        }

        const profile = userProfiles.get(socket.username);
        if (!profile || !profile.inventory || !profile.inventory.gifts) {
            socket.emit('error', 'У вас нет подарков в инвентаре');
            return;
        }

        if (!profile.inventory.gifts[gift] || profile.inventory.gifts[gift] <= 0) {
            socket.emit('error', 'У вас нет этого подарка в инвентаре');
            return;
        }

        const giftInfo = giftTypes[gift];
        if (!giftInfo) {
            socket.emit('error', 'Неверный тип подарка');
            return;
        }

        // Remove from inventory
        profile.inventory.gifts[gift]--;

        // Send the gift (similar to existing gift sending logic)
        if (recipient === 'all') {
            const onlineUsers = Array.from(users.keys()).filter(u => u !== socket.username);
            
            onlineUsers.forEach(user => {
                const recipientProfile = userProfiles.get(user);
                if (recipientProfile) {
                    if (!recipientProfile.receivedGifts) recipientProfile.receivedGifts = [];
                    if (!recipientProfile.favoriteGifts) recipientProfile.favoriteGifts = [];
                    if (!recipientProfile.badges) recipientProfile.badges = [];

                    const giftData = {
                        gift,
                        from: socket.username,
                        to: user,
                        timestamp: new Date(),
                        rarity: giftInfo.rarity,
                        value: giftInfo.value,
                        animation: giftInfo.animation,
                        badge: giftInfo.badge
                    };

                    recipientProfile.receivedGifts.push(giftData);
                    recipientProfile.giftPoints = (recipientProfile.giftPoints || 0) + giftInfo.value;

                    if (giftInfo.badge && !recipientProfile.badges.includes(giftInfo.badge)) {
                        recipientProfile.badges.push(giftInfo.badge);
                    }

                    const giftCount = recipientProfile.favoriteGifts.find(g => g && g.gift === gift);
                    if (giftCount) {
                        giftCount.count++;
                    } else {
                        recipientProfile.favoriteGifts.push({ gift, count: 1 });
                    }

                    const recipientUser = users.get(user);
                    if (recipientUser) {
                        io.to(recipientUser.socketId).emit('gift animation', {
                            gift,
                            animation: giftInfo.animation,
                            from: socket.username,
                            giftInfo
                        });
                        io.to(recipientUser.socketId).emit('gift received', {
                            gift,
                            giftInfo,
                            from: socket.username,
                            timestamp: new Date(),
                            badge: giftInfo.badge
                        });
                    }
                }
            });

            io.emit('send message', {
                message: `${socket.username} отправил ${giftInfo.name} ${gift} всем участникам чата из своего инвентаря! 🎁✨`,
                user: "Система",
                type: 'gift'
            });

        } else {
            const recipientProfile = userProfiles.get(recipient);
            if (!recipientProfile) {
                socket.emit('error', 'Получатель не найден');
                return;
            }

            if (!recipientProfile.receivedGifts) recipientProfile.receivedGifts = [];
            if (!recipientProfile.favoriteGifts) recipientProfile.favoriteGifts = [];
            if (!recipientProfile.badges) recipientProfile.badges = [];

            const giftData = {
                gift,
                from: socket.username,
                to: recipient,
                timestamp: new Date(),
                rarity: giftInfo.rarity,
                value: giftInfo.value,
                animation: giftInfo.animation,
                badge: giftInfo.badge
            };

            recipientProfile.receivedGifts.push(giftData);
            recipientProfile.giftPoints = (recipientProfile.giftPoints || 0) + giftInfo.value;

            if (giftInfo.badge && !recipientProfile.badges.includes(giftInfo.badge)) {
                recipientProfile.badges.push(giftInfo.badge);
            }

            const giftCount = recipientProfile.favoriteGifts.find(g => g && g.gift === gift);
            if (giftCount) {
                giftCount.count++;
            } else {
                recipientProfile.favoriteGifts.push({ gift, count: 1 });
            }

            io.emit('send message', {
                message: `${socket.username} отправил ${giftInfo.name} ${gift} для ${recipient} из инвентаря! 🎁`,
                user: "Система",
                type: 'gift'
            });

            const recipientUser = users.get(recipient);
            if (recipientUser) {
                io.to(recipientUser.socketId).emit('gift animation', {
                    gift,
                    animation: giftInfo.animation,
                    from: socket.username,
                    giftInfo
                });
                io.to(recipientUser.socketId).emit('gift received', {
                    gift,
                    giftInfo,
                    from: socket.username,
                    timestamp: new Date(),
                    badge: giftInfo.badge
                });
            }
        }

        // Update sender's inventory
        socket.emit('inventory updated', profile.inventory.gifts);
        
        saveData();
    });

    // Exchange system - convert gift points to coins
    socket.on('exchange gifts to coins', () => {
        if (!socket.username || !users.has(socket.username)) {
            socket.emit('error', 'Необходима авторизация');
            return;
        }

        const profile = userProfiles.get(socket.username);
        if (!profile) {
            socket.emit('error', 'Профиль не найден');
            return;
        }

        const giftPoints = profile.giftPoints || 0;
        if (giftPoints < 100) {
            socket.emit('error', 'Минимум 100 очков подарков для обмена');
            return;
        }

        // Exchange rate: 10 gift points = 1 coin
        const coinsToAdd = Math.floor(giftPoints / 10);
        profile.giftPoints = giftPoints % 10; // Keep remainder
        profile.coins = (profile.coins || 0) + coinsToAdd;

        // Update user coins in active users
        if (users.has(socket.username)) {
            users.get(socket.username).coins = profile.coins;
        }

        socket.emit('exchange success', {
            coinsReceived: coinsToAdd,
            newCoins: profile.coins,
            remainingGiftPoints: profile.giftPoints
        });

        io.emit('send message', {
            message: `${socket.username} обменял очки подарков на ${coinsToAdd} монет! 💰`,
            user: "Система",
            type: 'system'
        });

        saveData();
    });

    // Get gift inventory
    socket.on('get inventory', () => {
        if (!socket.username || !users.has(socket.username)) {
            socket.emit('error', 'Необходима авторизация');
            return;
        }

        const profile = userProfiles.get(socket.username);
        if (profile) {
            socket.emit('inventory data', profile.inventory || { gifts: {}, boosters: {}, themes: [], effects: [] });
        }
    });

    socket.on('update bio', (newBio) => {
        if (!socket.username) return;

        const profile = userProfiles.get(socket.username);
        if (profile && newBio.length <= 200) {
            profile.bio = newBio;
            socket.emit('bio updated', newBio);
            saveData();
        }
    });

    // Update settings
    socket.on('update settings', (settings) => {
        if (!socket.username) return;

        const profile = userProfiles.get(socket.username);
        if (profile) {
            profile.settings = { ...profile.settings, ...settings };
            socket.emit('settings updated', profile.settings);
            saveData();
        }
    });

    // Change user badge
    socket.on('change badge', (data) => {
        if (!socket.username || !users.has(socket.username)) {
            socket.emit('error', 'Необходима авторизация');
            return;
        }

        const { badgeId } = data;
        const profile = userProfiles.get(socket.username);

        if (!profile) {
            socket.emit('error', 'Профиль не найден');
            return;
        }

        // Check if user has this badge
        if (!profile.badges || !profile.badges.includes(badgeId)) {
            socket.emit('error', 'У вас нет этого бейджика');
            return;
        }

        // Set active badge
        profile.activeBadge = badgeId;

        // Update user data
        if (users.has(socket.username)) {
            users.get(socket.username).activeBadge = badgeId;
        }

        socket.emit('badge changed', { badge: badgeId });
        io.emit('users update', getUsersData());
        saveData();
    });

    // Get user gifts for profile display
    socket.on('get user gifts', (username) => {
        const profile = userProfiles.get(username || socket.username);

        if (!profile) {
            socket.emit('error', 'Профиль не найден');
            return;
        }

        const receivedGifts = profile.receivedGifts || [];
        const sentGifts = profile.sentGifts || [];

        // Group gifts by type and count
        const receivedGiftsData = {};
        receivedGifts.forEach(gift => {
            const giftType = gift.gift || gift;
            if (!receivedGiftsData[giftType]) {
                receivedGiftsData[giftType] = {
                    count: 0,
                    info: giftTypes[giftType] || { name: 'Неизвестный подарок', rarity: 'common' },
                    senders: []
                };
            }
            receivedGiftsData[giftType].count++;
            if (gift.from && !receivedGiftsData[giftType].senders.includes(gift.from)) {
                receivedGiftsData[giftType].senders.push(gift.from);
            }
        });

        const sentGiftsData = {};
        sentGifts.forEach(gift => {
            const giftType = gift.gift || gift;
            if (!sentGiftsData[giftType]) {
                sentGiftsData[giftType] = {
                    count: 0,
                    info: giftTypes[giftType] || { name: 'Неизвестный подарок', rarity: 'common' },
                    recipients: []
                };
            }
            sentGiftsData[giftType].count++;
            if (gift.to && !sentGiftsData[giftType].recipients.includes(gift.to)) {
                sentGiftsData[giftType].recipients.push(gift.to);
            }
        });

        socket.emit('user gifts data', {
            username: username || socket.username,
            receivedGifts: receivedGiftsData,
            sentGifts: sentGiftsData,
            totalReceived: receivedGifts.length,
            totalSent: sentGifts.length,
            giftPoints: profile.giftPoints || 0
        });
    });

    // Admin panel functions for avasfge
    socket.on('admin full panel', () => {
        if (!socket.username || socket.username !== 'avasfge') {
            socket.emit('error', 'Доступ запрещен');
            return;
        }

        const fullStats = {
            // Server stats
            serverUptime: process.uptime(),
            memoryUsage: process.memoryUsage(),
            nodeVersion: process.version,

            // User statistics
            totalUsers: userProfiles.size,
            onlineUsers: users.size,
            bannedUsers: bannedUsers.size,
            moderators: Array.from(moderators),

            // Activity stats
            totalMessages: Array.from(userProfiles.values()).reduce((sum, p) => sum + (p.totalMessages || 0), 0),
            totalGifts: Array.from(userProfiles.values()).reduce((sum, p) => sum + (p.sentGifts?.length || 0), 0),
            totalCoins: Array.from(userProfiles.values()).reduce((sum, p) => sum + (p.coins || 0), 0),

            // Recent activity
            recentJoins: Array.from(userProfiles.entries())
                .filter(([_, p]) => new Date(p.joinDate) > new Date(Date.now() - 24 * 60 * 60 * 1000))
                .length,

            // Top users
            topByLevel: Array.from(userProfiles.entries())
                .sort(([,a], [,b]) => (b.level || 1) - (a.level || 1))
                .slice(0, 10)
                .map(([username, profile]) => ({
                    username,
                    level: profile.level || 1,
                    xp: profile.xp || 0,
                    totalMessages: profile.totalMessages || 0
                })),

            topByGifts: Array.from(userProfiles.entries())
                .sort(([,a], [,b]) => (b.giftPoints || 0) - (a.giftPoints || 0))
                .slice(0, 10)
                .map(([username, profile]) => ({
                    username,
                    giftPoints: profile.giftPoints || 0,
                    receivedGifts: profile.receivedGifts?.length || 0,
                    sentGifts: profile.sentGifts?.length || 0
                }))
        };

        socket.emit('admin full stats', fullStats);
    });

    // Give user coins (admin only)
    socket.on('admin give coins', (data) => {
        if (!socket.username || socket.username !== 'avasfge') {
            socket.emit('error', 'Доступ запрещен');
            return;
        }

        const { username, amount } = data;
        const profile = userProfiles.get(username);

        if (!profile) {
            socket.emit('error', 'Пользователь не найден');
            return;
        }

        profile.coins = (profile.coins || 0) + amount;

        if (users.has(username)) {
            users.get(username).coins = profile.coins;
            const targetSocket = Array.from(io.sockets.sockets.values())
                .find(s => s.username === username);
            if (targetSocket) {
                targetSocket.emit('coins updated', profile.coins);
            }
        }

        socket.emit('admin action success', `Добавлено ${amount} монет пользователю ${username}`);
        saveData();
    });

    // Set user level (admin only)
    socket.on('admin set level', (data) => {
        if (!socket.username || socket.username !== 'avasfge') {
            socket.emit('error', 'Доступ запрещен');
            return;
        }

        const { username, level } = data;
        const profile = userProfiles.get(username);

        if (!profile) {
            socket.emit('error', 'Пользователь не найден');
            return;
        }

        profile.level = level;
        profile.xp = level * 100; // Set XP accordingly

        if (users.has(username)) {
            users.get(username).level = level;
        }

        socket.emit('admin action success', `Установлен ${level} уровень пользователю ${username}`);
        io.emit('users update', getUsersData());
        saveData();
    });

    // Give user badge (admin only)
    socket.on('admin give badge', (data) => {
        if (!socket.username || socket.username !== 'avasfge') {
            socket.emit('error', 'Доступ запрещен');
            return;
        }

        const { username, badge } = data;
        const profile = userProfiles.get(username);

        if (!profile) {
            socket.emit('error', 'Пользователь не найден');
            return;
        }

        if (!profile.badges) profile.badges = [];
        if (!profile.badges.includes(badge)) {
            profile.badges.push(badge);
            socket.emit('admin action success', `Добавлен бейджик ${badge} пользователю ${username}`);
        } else {
            socket.emit('error', 'У пользователя уже есть этот бейджик');
        }

        io.emit('users update', getUsersData());
        saveData();
    });

    socket.on('force disconnect', (reason) => {
        console.log(`Force disconnecting ${socket.username}: ${reason}`);
        socket.disconnect();
    });

    socket.on('disconnect', (reason) => {
        console.log(`Socket disconnected: ${socket.id}, reason: ${reason}`);
        if (socket.username) {
            console.log(`User ${socket.username} disconnected`);
            onlineStatus.set(socket.username, new Date());

            // Remove from active users
            if (users.has(socket.username)) {
                users.delete(socket.username);
                userSessions.delete(socket.username);

                io.emit('send message', {
                    message: `${socket.username} покинул чат`,
                    user: "Система",
                    type: 'system'
                });

                // Broadcast updated user list
                io.emit('users update', getUsersData());

                console.log(`Removed ${socket.username} from active users. Online users: ${users.size}`);
            }
            saveData();
        }
    });
});

function getUsersData() {
    return Array.from(users.entries()).map(([username, userData]) => {
        const profile = userProfiles.get(username);
        return {
            username,
            ...userData,
            isOnline: true,
            badges: profile ? profile.badges || [] : [],
            activeBadge: profile ? profile.activeBadge : null
        };
    });
}

function getPrivateChatsForUser(username) {
    const userChats = {};
    privateChats.forEach((messages, chatId) => {
        if (chatId.includes(username)) {
            userChats[chatId] = messages;
        }
    });
    return userChats;
}

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('Saving data before shutdown...');
    saveData();
    process.exit(0);
});
