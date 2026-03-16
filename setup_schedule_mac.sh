#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  snEco — МойСклад Auto Sync Setup (Mac)
#  Запускає moysklad_sync.py щодня о 06:00
#  Запуск: bash setup_schedule_mac.sh
# ═══════════════════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_ID="com.sneco.moysklad-sync"
PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_ID.plist"
LOG_DIR="$SCRIPT_DIR/logs"
PYTHON=$(which python3)

echo ""
echo "═══════════════════════════════════════════════"
echo "  snEco — МойСклад Auto Sync Setup"
echo "═══════════════════════════════════════════════"
echo "  Папка:  $SCRIPT_DIR"
echo "  Python: $PYTHON"
echo "  Розклад: щодня о 06:00"
echo "═══════════════════════════════════════════════"
echo ""

# ── 1. Встановити залежності ─────────────────────────────
echo "▶ Встановлюю Python-залежності..."
pip3 install requests pandas openpyxl python-dotenv --quiet
echo "  ✅ Готово"

# ── 2. Створити папку для логів ──────────────────────────
mkdir -p "$LOG_DIR"
echo "  ✅ Папка logs/ створена"

# ── 3. Видалити старий агент якщо є ─────────────────────
if launchctl list | grep -q "$PLIST_ID" 2>/dev/null; then
    echo "▶ Видаляю старий агент..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
fi

# ── 4. Створити plist ────────────────────────────────────
echo "▶ Створюю launchd агент..."

cat > "$PLIST_PATH" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$PLIST_ID</string>

    <key>ProgramArguments</key>
    <array>
        <string>$PYTHON</string>
        <string>$SCRIPT_DIR/moysklad_sync.py</string>
    </array>

    <key>WorkingDirectory</key>
    <string>$SCRIPT_DIR</string>

    <!-- Щодня о 06:00 -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>$LOG_DIR/moysklad_sync.log</string>

    <key>StandardErrorPath</key>
    <string>$LOG_DIR/moysklad_sync_error.log</string>

    <!-- Запускати лише якщо є мережа -->
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
EOF

echo "  ✅ Файл: $PLIST_PATH"

# ── 5. Активувати агент ──────────────────────────────────
echo "▶ Активую агент..."
launchctl load "$PLIST_PATH"
echo "  ✅ Агент активовано"

# ── 6. Тестовий запуск ───────────────────────────────────
echo ""
echo "▶ Тестовий запуск (перша синхронізація)..."
echo "  Це може зайняти кілька хвилин..."
echo ""
cd "$SCRIPT_DIR"
$PYTHON moysklad_sync.py

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ Все налаштовано!"
echo ""
echo "  📅 Синхронізація: щодня о 06:00"
echo "  📁 Дані:  $SCRIPT_DIR/data/"
echo "  📋 Логи:  $SCRIPT_DIR/logs/"
echo ""
echo "  Корисні команди:"
echo "  • Запустити вручну:  python3 $SCRIPT_DIR/moysklad_sync.py"
echo "  • Переглянути лог:   cat $SCRIPT_DIR/logs/moysklad_sync.log"
echo "  • Вимкнути розклад:  launchctl unload $PLIST_PATH"
echo "  • Увімкнути знову:   launchctl load $PLIST_PATH"
echo "═══════════════════════════════════════════════"
echo ""
