import sqlite3
import os
import sys
from typing import Optional

DB_NAME = 'resistance.db'

# База данных утилита

def get_conn():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON;')
    return conn


def init_db(seed: bool = True):
    first_time = not os.path.exists(DB_NAME)
    conn = get_conn()
    cur = conn.cursor()

    # создание таблицы
    cur.executescript('''
    CREATE TABLE IF NOT EXISTS agents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codename TEXT UNIQUE NOT NULL,
        rank INTEGER NOT NULL CHECK(rank >= 1),
        skill TEXT,
        alive INTEGER NOT NULL CHECK(alive IN (0,1)) DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS missions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        difficulty INTEGER NOT NULL CHECK(difficulty BETWEEN 1 AND 10),
        status TEXT NOT NULL CHECK(status IN ('planned','in progress','failed','success')) DEFAULT 'planned',
        assigned_agent INTEGER,
        FOREIGN KEY(assigned_agent) REFERENCES agents(id) ON DELETE CASCADE
    );
    ''')
    conn.commit()

    if seed and first_time:
        seed_data(conn)

    conn.close()


# проверка на уникальность позывной

def safe_insert_agent(conn, codename, rank, skill, alive=1):
    cur = conn.cursor()
    try:
        cur.execute('INSERT INTO agents (codename, rank, skill, alive) VALUES (?,?,?,?)',
                    (codename, rank, skill, alive))
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError as e:
        print(f"Ошибка добавления агента '{codename}': {e}")
        return None


def safe_insert_mission(conn, title, difficulty, status, assigned_agent_id: Optional[int]):
    cur = conn.cursor()
    # проверка валидацию: при назначении миссии учитывать, что агент не может иметь одновременно больше 3 активных миссий
    if assigned_agent_id is not None:
        ag = cur.execute('SELECT * FROM agents WHERE id = ?', (assigned_agent_id,)).fetchone()
        if ag is None:
            print(f"Агент с id={assigned_agent_id} не найден. Миссия не добавлена.")
            return None
        if ag['alive'] == 0:
            print(f"Нельзя назначить миссию погибшему агенту ({ag['codename']}).")
            return None
        count_inprogress = cur.execute("SELECT COUNT(*) FROM missions WHERE assigned_agent = ? AND status = 'in progress'",
                                       (assigned_agent_id,)).fetchone()[0]
        if status == 'in progress' and count_inprogress >= 3:
            print(f"Нельзя назначить: агент {ag['codename']} уже имеет 3 активные миссии.")
            return None

    try:
        cur.execute('INSERT INTO missions (title, difficulty, status, assigned_agent) VALUES (?,?,?,?)',
                    (title, difficulty, status, assigned_agent_id))
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError as e:
        print(f"Ошибка добавления миссии '{title}': {e}")
        return None

def seed_data(conn):
    print('Добавляются тестовые данные...')
    agents = [
        ('Тень', 5, 'взлом систем', 1),
        ('Гадюка', 3, 'скрытность', 1),
        ('Богомол', 7, 'бой', 1),
        ('Оракул', 4, 'разведка', 1),
        ('Призрак', 2, 'инфильтрация', 1),
        ('Ладья', 6, 'взрывное дело', 1),
    ]
    agent_ids = []
    for codename, rank, skill, alive in agents:
        aid = safe_insert_agent(conn, codename, rank, skill, alive)
        agent_ids.append(aid)

    missions = [
        ('Украсть кредиты корпорации', 6, 'planned', agent_ids[0]),
        ('Установить маяк', 4, 'in progress', agent_ids[1]),
        ('Пустить под откос конвой', 8, 'planned', agent_ids[2]),
        ('Сбор разведданных', 3, 'success', agent_ids[3]),
        ('Тихое проникновение', 5, 'failed', agent_ids[4]),
        ('Обезвредить бомбу', 7, 'in progress', agent_ids[5]),
        ('Кража данных корпорации', 9, 'planned', agent_ids[0]),
        ('Спасти агента', 6, 'in progress', agent_ids[2]),
        ('Саботаж серверов', 10, 'planned', agent_ids[5]),
        ('Отвлечь охрану', 2, 'success', agent_ids[1]),
    ]

    for title, diff, status, aid in missions:
        safe_insert_mission(conn, title, diff, status, aid)


# агенты

def list_agents(order_desc_rank: bool = True):
    conn = get_conn()
    cur = conn.cursor()
    order = 'DESC' if order_desc_rank else 'ASC'
    rows = cur.execute(f'SELECT * FROM agents ORDER BY rank {order}').fetchall()
    conn.close()
    return rows


def list_alive_agents_with_rank_above(n: int):
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('SELECT * FROM agents WHERE alive = 1 AND rank > ? ORDER BY rank DESC', (n,)).fetchall()
    conn.close()
    return rows


def add_agent_interactive():
    conn = get_conn()
    codename = input('Codename: ').strip()
    rank = int(input('Rank (>=1): ').strip())
    skill = input('Skill: ').strip()
    alive = 1
    aid = safe_insert_agent(conn, codename, rank, skill, alive)
    conn.close()
    if aid:
        print('Агент добавлен, id=', aid)


def mark_agent_dead(codename_or_id):
    conn = get_conn()
    cur = conn.cursor()
    if isinstance(codename_or_id, int) or codename_or_id.isdigit():
        cur.execute('UPDATE agents SET alive = 0 WHERE id = ?', (int(codename_or_id),))
    else:
        cur.execute('UPDATE agents SET alive = 0 WHERE codename = ?', (codename_or_id,))
    conn.commit()
    conn.close()
    print('Агент помечен как погибший (если был).')


def delete_dead_agent_by_id(agent_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM agents WHERE id = ? AND alive = 0', (agent_id,))
    conn.commit()
    affected = cur.rowcount
    conn.close()
    print(f'Удалено агентов: {affected} (только погибшие могли быть удалены).')


def promote_agent(codename_or_id, delta=1):
    conn = get_conn()
    cur = conn.cursor()
    if isinstance(codename_or_id, int) or str(codename_or_id).isdigit():
        cur.execute('UPDATE agents SET rank = rank + ? WHERE id = ?', (delta, int(codename_or_id)))
    else:
        cur.execute('UPDATE agents SET rank = rank + ? WHERE codename = ?', (delta, codename_or_id))
    conn.commit()
    conn.close()
    print('Ранг агента изменён (если найден).')


# миссия

def list_missions_with_agents():
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT m.*, a.codename AS agent_codename
        FROM missions m
        LEFT JOIN agents a ON m.assigned_agent = a.id
        ORDER BY m.id
    ''').fetchall()
    conn.close()
    return rows


def missions_with_max_difficulty():
    conn = get_conn()
    cur = conn.cursor()
    maxd = cur.execute('SELECT MAX(difficulty) FROM missions').fetchone()[0]
    rows = cur.execute('''
        SELECT m.*, a.codename AS agent_codename
        FROM missions m
        LEFT JOIN agents a ON m.assigned_agent = a.id
        WHERE m.difficulty = ?
    ''', (maxd,)).fetchall()
    conn.close()
    return maxd, rows


def change_mission_status(mission_id: int, new_status: str):
    if new_status not in ('planned', 'in progress', 'failed', 'success'):
        print('Неверный статус')
        return
    conn = get_conn()
    cur = conn.cursor()
    m = cur.execute('SELECT * FROM missions WHERE id = ?', (mission_id,)).fetchone()
    if not m:
        print('Миссия не найдена')
        conn.close()
        return
    assigned = m['assigned_agent']
    if new_status == 'in progress' and assigned is not None:
        count_inprogress = cur.execute("SELECT COUNT(*) FROM missions WHERE assigned_agent = ? AND status = 'in progress'",
                                       (assigned,)).fetchone()[0]
        was_inprogress = (m['status'] == 'in progress')
        if not was_inprogress and count_inprogress >= 3:
            ag = cur.execute('SELECT codename FROM agents WHERE id = ?', (assigned,)).fetchone()
            print(f"Нельзя перевести в 'in progress' — агент {ag['codename']} имеет уже 3 активных миссии.")
            conn.close()
            return
    cur.execute('UPDATE missions SET status = ? WHERE id = ?', (new_status, mission_id))
    conn.commit()
    conn.close()
    print('Статус миссии обновлён.')


def add_mission_interactive():
    conn = get_conn()
    title = input('Title: ').strip()
    difficulty = int(input('Difficulty (1-10): ').strip())
    status = input("Status (planned,in progress,failed,success) [planned]: ").strip() or 'planned'
    assigned = input('Assigned agent id (or empty): ').strip()
    assigned_id = int(assigned) if assigned else None
    mid = safe_insert_mission(conn, title, difficulty, status, assigned_id)
    conn.close()
    if mid:
        print('Миссия добавлена, id=', mid)


def delete_failed_missions_above_difficulty(threshold: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM missions WHERE status = "failed" AND difficulty > ?', (threshold,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    print(f'Удалено миссий: {deleted}')


# аналитика

def missions_count_per_agent():
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT a.id, a.codename, COUNT(m.id) AS total
        FROM agents a
        LEFT JOIN missions m ON a.id = m.assigned_agent
        GROUP BY a.id
        ORDER BY total DESC
    ''').fetchall()
    conn.close()
    return rows


def agents_with_at_least_k_missions(k: int):
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT a.id, a.codename, COUNT(m.id) AS total
        FROM agents a
        LEFT JOIN missions m ON a.id = m.assigned_agent
        GROUP BY a.id
        HAVING total >= ?
    ''', (k,)).fetchall()
    conn.close()
    return rows


def agent_with_highest_success_rate():
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT a.id, a.codename,
               SUM(CASE WHEN m.status = 'success' THEN 1 ELSE 0 END) AS success_cnt,
               COUNT(m.id) AS total
        FROM agents a
        LEFT JOIN missions m ON a.id = m.assigned_agent
        GROUP BY a.id
        HAVING total > 0
    ''').fetchall()
    conn.close()
    best = None
    best_rate = -1
    for r in rows:
        rate = r['success_cnt'] / r['total']
        if rate > best_rate:
            best_rate = rate
            best = (r['id'], r['codename'], r['success_cnt'], r['total'], rate)
    return best


def build_report_table():
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT a.codename,
               COUNT(m.id) AS total,
               SUM(CASE WHEN m.status='success' THEN 1 ELSE 0 END) AS success,
               SUM(CASE WHEN m.status='failed' THEN 1 ELSE 0 END) AS failed,
               CASE WHEN COUNT(m.id)=0 THEN 0.0 ELSE ROUND(100.0 * SUM(CASE WHEN m.status='success' THEN 1 ELSE 0 END) / COUNT(m.id),2) END AS success_pct
        FROM agents a
        LEFT JOIN missions m ON a.id = m.assigned_agent
        GROUP BY a.id
        ORDER BY success_pct DESC
    ''').fetchall()
    conn.close()
    return rows


def agents_with_more_failed_than_success():
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT a.codename,
               SUM(CASE WHEN m.status='success' THEN 1 ELSE 0 END) AS success,
               SUM(CASE WHEN m.status='failed' THEN 1 ELSE 0 END) AS failed
        FROM agents a
        LEFT JOIN missions m ON a.id = m.assigned_agent
        GROUP BY a.id
        HAVING failed > success
    ''').fetchall()
    conn.close()
    return rows


def agents_with_no_missions():
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute('''
        SELECT a.id, a.codename
        FROM agents a
        LEFT JOIN missions m ON a.id = m.assigned_agent
        GROUP BY a.id
        HAVING COUNT(m.id) = 0
    ''').fetchall()
    conn.close()
    return rows


# CLI интерфейс

def admin_menu():
    while True:
        print('\n--- Admin Menu ---')
        print('1) Добавить агента')
        print('2) Пометить агента погибшим')
        print('3) Удалить погибшего агента по id')
        print('4) Повысить ранг агента')
        print('5) Просмотреть аналитику (отчёт)')
        print('6) Список агентов')
        print('7) Список миссий с агентами')
        print('8) Агенты без миссий') 
        print('9) Агенты, у которых проваленных миссий больше, чем успешных')
        print('0) Выход')
        choice = input('> ').strip()
        
        if choice == '1':
            add_agent_interactive()
        elif choice == '2':
            x = input('Введите codename или id агента: ').strip()
            mark_agent_dead(x)
        elif choice == '3':
            aid = int(input('Введите id агента: ').strip())
            delete_dead_agent_by_id(aid)
        elif choice == '4':
            x = input('Введите codename или id: ').strip()
            delta = int(input('На сколько повысить (по умолчанию 1): ') or '1')
            promote_agent(x, delta)
        elif choice == '5':  # Основной отчёт
            rpt = build_report_table()
            print(f"\n{'Codename':<10} | {'Всего':<5} | {'Успешных':<9} | {'Проваленных':<11} | {'% успеха':<8}")
            print("-"*55)
            for r in rpt:
                print(f"{r['codename']:<10} | {r['total']:<5} | {r['success']:<9} | {r['failed']:<11} | {r['success_pct']:<8}%")
        elif choice == '6':
            rows = list_agents()
            print(f"\n{'ID':<3} | {'Codename':<10} | {'Rank':<4} | {'Skill':<15} | {'Alive':<5}")
            print("-"*45)
            for r in rows:
                print(f"{r['id']:<3} | {r['codename']:<10} | {r['rank']:<4} | {r['skill']:<15} | {r['alive']:<5}")
        elif choice == '7':
            ms = list_missions_with_agents()
            print(f"\n{'ID':<3} | {'Title':<25} | {'Diff':<4} | {'Status':<11} | {'Agent':<10}")
            print("-"*65)
            for m in ms:
                agent_name = m['agent_codename'] if m['agent_codename'] else '-'
                print(f"{m['id']:<3} | {m['title']:<25} | {m['difficulty']:<4} | {m['status']:<11} | {agent_name:<10}")
        elif choice == '8':  # Агенты без миссий
            rows = agents_with_no_missions()
            if not rows:
                print("Все агенты назначены на миссии.")
            else:
                print(f"\n{'ID':<3} | {'Codename':<10}")
                print("-"*15)
                for r in rows:
                    print(f"{r['id']:<3} | {r['codename']:<10}")
        elif choice == '9':  # Агенты с failed > success
            rows = agents_with_more_failed_than_success()
            if not rows:
                print("Нет агентов с количеством проваленных миссий больше успешных.")
            else:
                print(f"\n{'Codename':<10} | {'Успешных':<9} | {'Проваленных':<11}")
                print("-"*35)
                for r in rows:
                    print(f"{r['codename']:<10} | {r['success']:<9} | {r['failed']:<11}")
        elif choice == '0':
            break
        else:
            print('Неверный выбор')

def operator_menu():
    while True:
        print('\n--- Operator Menu ---')
        print('1) Добавить миссию')
        print('2) Изменить статус миссии')
        print('3) Посмотреть список агентов')
        print('4) Посмотреть список миссий')
        print('0) Выход')
        choice = input('> ').strip()
        
        if choice == '1':
            add_mission_interactive()
        elif choice == '2':
            mid = int(input('ID миссии: ').strip())
            ns = input("Новый статус (planned,in progress,failed,success): ").strip()
            change_mission_status(mid, ns)
        elif choice == '3':
            rows = list_agents()
            print(f"\n{'ID':<3} | {'Codename':<10} | {'Rank':<4} | {'Skill':<15} | {'Alive':<5}")
            print("-"*45)
            for r in rows:
                print(f"{r['id']:<3} | {r['codename']:<10} | {r['rank']:<4} | {r['skill']:<15} | {r['alive']:<5}")
        elif choice == '4':
            ms = list_missions_with_agents()
            print(f"\n{'ID':<3} | {'Title':<25} | {'Diff':<4} | {'Status':<11} | {'Agent':<10}")
            print("-"*65)
            for m in ms:
                agent_name = m['agent_codename'] if m['agent_codename'] else '-'
                print(f"{m['id']:<3} | {m['title']:<25} | {m['difficulty']:<4} | {m['status']:<11} | {agent_name:<10}")
        elif choice == '0':
            break
        else:
            print('Неверный выбор')


def main():
    init_db(seed=True)
    print('Добро пожаловать в систему Сопротивления (CLI)')
    while True:
        print('\nВыберите роль:')
        print('1) Admin')
        print('2) Operator')
        print('0) Выйти')
        role = input('> ').strip()
        if role == '1':
            admin_menu()
        elif role == '2':
            operator_menu()
        elif role == '0':
            print('Пока.')
            break
        else:
            print('Неверный ввод')


if __name__ == '__main__':
    main()
