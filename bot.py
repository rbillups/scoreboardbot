import os
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from sqlalchemy import BigInteger

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from sqlalchemy.engine import make_url
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean,
    ForeignKey, create_engine, func, select, and_, or_
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, scoped_session

# ------------- Config -------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")  # optional, speeds up slash-command sync for one server
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMINS", "").split(",") if x.strip().isdigit()}

# Use minimal intents (no privileged ones needed for slash commands)
INTENTS = discord.Intents.default()

from sqlalchemy import text

# ------------- DB Setup -------------
Base = declarative_base()

# Always use an ephemeral SQLite DB on Heroku (resets on dyno restart)
DB_PATH = os.getenv("SQLITE_PATH", "/tmp/records.db")
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    future=True,
    connect_args={"check_same_thread": False},  # SQLite + threads
)

# Enforce foreign keys on SQLite
from sqlalchemy import event
@event.listens_for(engine, "connect")
def _fk_pragma(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA foreign_keys=ON")
    cur.close()

SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))

def now_utc():
    return datetime.now(timezone.utc)

class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True)  # Discord user ID
    display_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=now_utc)

class Game(Base):
    __tablename__ = "games"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    short_code = Column(String, unique=True, nullable=False)

class Season(Base):
    __tablename__ = "seasons"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    status = Column(String, default="active")  # active|closed
    game_id = Column(Integer, ForeignKey("games.id"), nullable=True)
    started_at = Column(DateTime, default=now_utc)
    ended_at = Column(DateTime, nullable=True)

    game = relationship("Game")

class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey("games.id"), nullable=False)
    season_id = Column(Integer, ForeignKey("seasons.id"), nullable=True)

    reporter_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    winner_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    loser_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)

    score_w = Column(Integer, nullable=True)
    score_l = Column(Integer, nullable=True)

    played_at = Column(DateTime, default=now_utc)
    verified = Column(Boolean, default=True)
    voided = Column(Boolean, default=False)
    dupe_of = Column(Integer, ForeignKey("matches.id"), nullable=True)

    game = relationship("Game")
    season = relationship("Season", foreign_keys=[season_id])
    reporter = relationship("User", foreign_keys=[reporter_id])
    winner = relationship("User", foreign_keys=[winner_id])
    loser = relationship("User", foreign_keys=[loser_id])

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    who_id = Column(BigInteger, nullable=False)
    action = Column(String, nullable=False)
    created_at = Column(DateTime, default=now_utc)

Base.metadata.create_all(engine)

# ------------- Helpers -------------
def _norm_code(s: str) -> str:
    """Normalize to a lowercase, no-space short code (e.g., 'M a d d e n' -> 'madden')."""
    return "".join(str(s).lower().split())

def upsert_user(session, member: discord.abc.User) -> User:
    """Create or update a User row, safely handling multiple references to the same user in one transaction."""
    name = getattr(member, "display_name", None) or getattr(member, "global_name", None) or member.name

    u = session.get(User, member.id)
    if u is None:
        for obj in session.new:
            if isinstance(obj, User) and obj.id == member.id:
                u = obj
                break

    if u is None:
        u = User(id=member.id, display_name=name)
        session.add(u)
        session.flush([u])
    else:
        if name and u.display_name != name:
            u.display_name = name

    return u

def get_or_create_game(session, name_or_code: Optional[str]) -> Optional[Game]:
    if not name_or_code:
        return None
    raw = name_or_code.strip()
    code = _norm_code(raw)  # lowercase, no spaces
    # First try exact short_code match
    q = session.execute(select(Game).where(func.lower(Game.short_code) == code)).scalar_one_or_none()
    if q:
        return q
    # Then try case-insensitive name match
    q = session.execute(select(Game).where(func.lower(Game.name) == raw.lower())).scalar_one_or_none()
    if q:
        return q
    # Create new with normalized short_code and nice title name
    g = Game(name=raw.title(), short_code=code)
    session.add(g)
    session.flush()
    return g

def find_active_season(session, name: Optional[str], game: Optional[Game]) -> Optional[Season]:
    if name:
        stmt = select(Season).where(func.lower(Season.name) == name.lower(), Season.status == "active")
        if game:
            stmt = stmt.where(or_(Season.game_id == None, Season.game_id == game.id))
        return session.execute(stmt).scalar_one_or_none()
    else:
        stmt = select(Season).where(Season.status == "active")
        if game:
            stmt = stmt.where(or_(Season.game_id == None, Season.game_id == game.id))
        return session.execute(stmt.order_by(Season.started_at.desc())).scalar_one_or_none()

def season_filter_clause(game_id: Optional[int], season_name: Optional[str]):
    def add_filters(stmt):
        if game_id:
            stmt = stmt.where(Match.game_id == game_id)
        if season_name:
            stmt = stmt.join(Season, Season.id == Match.season_id).where(
                func.lower(Season.name) == season_name.lower()
            )
        return stmt
    return add_filters

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def dupe_match_exists(session, game_id: int, a_id: int, b_id: int, window_minutes=5) -> Optional[int]:
    """Return most recent duplicate match id within window, if any."""
    since = now_utc() - timedelta(minutes=window_minutes)
    stmt = (
        select(Match.id)
        .where(
            Match.game_id == game_id,
            Match.played_at >= since,
            Match.voided == False,
            or_(
                and_(Match.winner_id == a_id, Match.loser_id == b_id),
                and_(Match.winner_id == b_id, Match.loser_id == a_id),
            ),
        )
        .order_by(Match.id.desc())
        .limit(1)
    )
    return session.execute(stmt).scalars().first()

# ------------- Bot -------------
class RecordsBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=INTENTS)

    async def setup_hook(self):
        # Fast sync to one guild if provided (speeds up slash availability)
        if GUILD_ID and GUILD_ID.isdigit():
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

bot = RecordsBot()

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (id: {bot.user.id})")
    try:
        await bot.change_presence(activity=discord.Game(name="/help"))
    except Exception:
        pass

# ----- Slash Commands -----

# Public: /report (silent dupe detection, log-only)
@bot.tree.command(name="report", description="Record a match result (win/loss).")
@app_commands.describe(
    game="Game name or code (e.g., 'madden')",
    winner="Select winner",
    loser="Select loser",
    score_w="Winner score (optional)",
    score_l="Loser score (optional)",
    season="Season name (optional; uses active season if available)"
)
async def report(
    interaction: discord.Interaction,
    game: str,
    winner: discord.User,
    loser: discord.User,
    score_w: Optional[int] = None,
    score_l: Optional[int] = None,
    season: Optional[str] = None
):
    if winner.id == loser.id:
        return await interaction.response.send_message("Winner and loser must be different users.")

    await interaction.response.defer()
    session = SessionLocal()

    try:
        g = get_or_create_game(session, game)

        # Ensure users exist / names updated (dedup across reporter/winner/loser)
        for m in {interaction.user.id: interaction.user, winner.id: winner, loser.id: loser}.values():
            upsert_user(session, m)

        u_reporter = session.get(User, interaction.user.id)
        u_winner   = session.get(User, winner.id)
        u_loser    = session.get(User, loser.id)

        # Auto-correct reversed scores
        if score_w is not None and score_l is not None and score_w < score_l:
            score_w, score_l = score_l, score_w
            u_winner, u_loser = u_loser, u_winner

        s = find_active_season(session, season, g) if season else None

        # Silent dupe check (log-only)
        dupe = dupe_match_exists(session, g.id, u_winner.id, u_loser.id)

        m = Match(
            game_id=g.id,
            season_id=s.id if s else None,
            reporter_id=u_reporter.id,
            winner_id=u_winner.id,
            loser_id=u_loser.id,
            score_w=score_w,
            score_l=score_l,
            verified=True,
            voided=False,
            dupe_of=dupe if dupe else None
        )
        session.add(m)

        action = f"report match {u_winner.display_name} vs {u_loser.display_name} in {g.short_code}"
        if dupe:
            action += f" [dupe_of:{dupe}]"
        session.add(AuditLog(who_id=u_reporter.id, action=action))

        session.commit()

        label = f"{g.name}" + (f" — {s.name}" if s else "")
        score_txt = f" {m.score_w}-{m.score_l}" if (m.score_w is not None and m.score_l is not None) else ""
        await interaction.followup.send(
            f"✅ Recorded: **{u_winner.display_name}** beat **{u_loser.display_name}**{score_txt} in **{label}**. Match ID: `{m.id}`"
        )
    except Exception as e:
        session.rollback()
        await interaction.followup.send(f"❌ Error recording match: {e}")
    finally:
        session.close()

# Public: /record (player, h2h, or leaderboard if no user given)
@bot.tree.command(name="record", description="Show a player's record, head-to-head, or a top-10 leaderboard.")
@app_commands.describe(
    game="Game name/code (optional)",
    user="Player to summarize (optional)",
    vs="Head-to-head vs another player (optional)",
    season="Season filter (optional)"
)
async def record(
    interaction: discord.Interaction,
    game: Optional[str] = None,
    user: Optional[discord.User] = None,
    vs: Optional[discord.User] = None,
    season: Optional[str] = None
):
    # Public response
    await interaction.response.defer()
    session = SessionLocal()
    try:
        g = get_or_create_game(session, game) if game else None
        add_filters = season_filter_clause(g.id if g else None, season)

        # Base scope: verified & not voided, plus optional game/season filters
        base = select(Match).where(Match.verified == True, Match.voided == False)
        base = add_filters(base)
        base_sub = base.subquery()

        def wl_for(uid: int, vs_uid: Optional[int]) -> Tuple[int, int]:
            if vs_uid:
                wins = session.execute(
                    select(func.count())
                    .select_from(base_sub)
                    .where(
                        base_sub.c.winner_id == uid,
                        base_sub.c.loser_id == vs_uid,
                    )
                ).scalar()
                losses = session.execute(
                    select(func.count())
                    .select_from(base_sub)
                    .where(
                        base_sub.c.winner_id == vs_uid,
                        base_sub.c.loser_id == uid,
                    )
                ).scalar()
            else:
                wins = session.execute(
                    select(func.count()).select_from(base_sub).where(base_sub.c.winner_id == uid)
                ).scalar()
                losses = session.execute(
                    select(func.count()).select_from(base_sub).where(base_sub.c.loser_id == uid)
                ).scalar()
            return (wins or 0), (losses or 0)

        g_label = g.name if g else "All Games"
        s_label = f", Season: {season}" if season else ""

        if user and vs:
            # Ensure names exist/are up to date
            for m in {user.id: user, vs.id: vs}.values():
                upsert_user(session, m)
            u1 = session.get(User, user.id)
            u2 = session.get(User, vs.id)
            w, l = wl_for(u1.id, u2.id)
            await interaction.followup.send(
                f"**Head-to-Head** {g_label}{s_label}\n{u1.display_name} vs {u2.display_name}: **{w}–{l}**"
            )

        elif user:
            upsert_user(session, user)
            u = session.get(User, user.id)
            w, l = wl_for(u.id, None)
            await interaction.followup.send(
                f"**Record** ({g_label}{s_label}) for **{u.display_name}**: **{w}–{l}**"
            )

        else:
            # Leaderboard (Top 10)
            wins_by = session.execute(
                select(base_sub.c.winner_id, func.count().label("w"))
                .select_from(base_sub)
                .group_by(base_sub.c.winner_id)
            ).all()
            losses_by = session.execute(
                select(base_sub.c.loser_id, func.count().label("l"))
                .select_from(base_sub)
                .group_by(base_sub.c.loser_id)
            ).all()

            w_map = {row[0]: row[1] for row in wins_by}
            l_map = {row[0]: row[1] for row in losses_by}
            user_ids = set(w_map.keys()) | set(l_map.keys())

            rows = []
            for uid in user_ids:
                w = w_map.get(uid, 0)
                l = l_map.get(uid, 0)
                winp = (w / (w + l)) * 100 if (w + l) > 0 else 0.0
                rows.append((uid, w, l, winp))

            # Sort: wins desc, losses asc, win% desc
            rows.sort(key=lambda r: (-r[1], r[2], -r[3]))
            rows = rows[:10]

            if not rows:
                await interaction.followup.send(f"No matches recorded yet for that scope ({g_label}{s_label}).")
            else:
                lines = []
                for i, (uid, w, l, wp) in enumerate(rows, 1):
                    u = session.get(User, uid)
                    name = u.display_name if u else str(uid)
                    lines.append(f"{i}. **{name}** — {w}–{l} ({wp:.0f}%)")
                s_tag = f" — Season: {season}" if season else ""
                await interaction.followup.send(f"**Top 10 — {g_label}{s_tag}**\n" + "\n".join(lines))

        session.commit()
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")
    finally:
        session.close()

# Public: convenience alias for H2H
@bot.tree.command(name="head2head", description="Head-to-head record between two players.")
@app_commands.describe(game="Game (optional)", user1="Player 1", user2="Player 2", season="Season (optional)")
async def head2head(
    interaction: discord.Interaction,
    user1: discord.User,
    user2: discord.User,
    game: Optional[str] = None,
    season: Optional[str] = None
):
    await record.callback(interaction, game=game, user=user1, vs=user2, season=season)

# Public: /leaderboard (routes to /record with no users)
@bot.tree.command(name="leaderboard", description="Show the top 10 players by record.")
@app_commands.describe(game="Game name/code (optional)", season="Season filter (optional)")
async def leaderboard(
    interaction: discord.Interaction,
    game: Optional[str] = None,
    season: Optional[str] = None
):
    await record.callback(interaction, game=game, user=None, vs=None, season=season)

# ----- Seasons -----

def require_admin(interaction: discord.Interaction):
    if not is_admin(interaction.user.id):
        raise PermissionError("Admin only.")

@bot.tree.command(name="season_start", description="Start a new season (admin).")
@app_commands.describe(name="Season name", game="Game name/code (optional)")
async def season_start(interaction: discord.Interaction, name: str, game: Optional[str] = None):
    try:
        require_admin(interaction)
    except Exception as e:
        return await interaction.response.send_message(f"❌ {e}", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    session = SessionLocal()
    try:
        g = get_or_create_game(session, game) if game else None
        s = Season(name=name, status="active", game_id=g.id if g else None, started_at=now_utc())
        session.add(s)
        session.add(AuditLog(who_id=interaction.user.id, action=f"season_start {name}"))
        session.commit()
        label = f"{name}" + (f" ({g.name})" if g else "")
        await interaction.followup.send(f"✅ Season started: **{label}**", ephemeral=True)
    except Exception as e:
        session.rollback()
        await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    finally:
        session.close()

@bot.tree.command(name="season_end", description="End a season (admin).")
@app_commands.describe(name="Season name")
async def season_end(interaction: discord.Interaction, name: str):
    try:
        require_admin(interaction)
    except Exception as e:
        return await interaction.response.send_message(f"❌ {e}", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    session = SessionLocal()
    try:
        s = session.execute(
            select(Season).where(func.lower(Season.name) == name.lower(), Season.status == "active")
        ).scalar_one_or_none()
        if not s:
            return await interaction.followup.send("Season not found or already closed.", ephemeral=True)
        s.status = "closed"
        s.ended_at = now_utc()
        session.add(AuditLog(who_id=interaction.user.id, action=f"season_end {name}"))
        session.commit()
        await interaction.followup.send(f"✅ Season ended: **{name}**", ephemeral=True)
    except Exception as e:
        session.rollback()
        await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    finally:
        session.close()

@bot.tree.command(name="season_reset", description="Reset (delete) all matches for a season (admin).")
@app_commands.describe(name="Season name")
async def season_reset(interaction: discord.Interaction, name: str):
    try:
        require_admin(interaction)
    except Exception as e:
        return await interaction.response.send_message(f"❌ {e}", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    session = SessionLocal()
    try:
        s = session.execute(select(Season).where(func.lower(Season.name) == name.lower())).scalar_one_or_none()
        if not s:
            return await interaction.followup.send("Season not found.", ephemeral=True)
        count = session.query(Match).filter(Match.season_id == s.id).delete()
        session.add(AuditLog(who_id=interaction.user.id, action=f"season_reset {name} ({count} matches)"))
        session.commit()
        await interaction.followup.send(f"🧹 Deleted **{count}** matches from season **{name}**.", ephemeral=True)
    except Exception as e:
        session.rollback()
        await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    finally:
        session.close()

# Public: matchup reset (admin-gated, but public success/error)
@bot.tree.command(name="matchup_reset", description="Admin: reset a head-to-head to 0–0 for a game (optional season).")
@app_commands.describe(
    user1="Player 1",
    user2="Player 2",
    game="Game name/code",
    season="Season name (optional)"
)
async def matchup_reset(
    interaction: discord.Interaction,
    user1: discord.User,
    user2: discord.User,
    game: str,
    season: Optional[str] = None
):
    # If a non-admin runs it, the error is public as requested
    try:
        require_admin(interaction)
    except Exception as e:
        return await interaction.response.send_message(f"❌ {e}")

    await interaction.response.defer()  # public

    session = SessionLocal()
    try:
        g = get_or_create_game(session, game)
        s = find_active_season(session, season, g) if season else None

        # Ensure users exist / names updated
        for m in {user1.id: user1, user2.id: user2}.values():
            upsert_user(session, m)

        q = session.query(Match).filter(
            Match.game_id == g.id,
            Match.voided == False,
            or_(
                and_(Match.winner_id == user1.id, Match.loser_id == user2.id),
                and_(Match.winner_id == user2.id, Match.loser_id == user1.id),
            )
        )
        if s:
            q = q.filter(Match.season_id == s.id)

        count = 0
        for m in q.all():
            m.voided = True
            count += 1

        session.add(AuditLog(
            who_id=interaction.user.id,
            action=f"matchup_reset {user1.id}<->{user2.id} in {g.short_code}" + (f" season {s.name}" if s else "")
        ))
        session.commit()

        label = f"{g.name}" + (f" — {s.name}" if s else "")
        await interaction.followup.send(
            f"🧹 Reset head-to-head for **{user1.display_name}** vs **{user2.display_name}** in **{label}**. "
            f"Voided **{count}** matches. Now 0–0."
        )
    except Exception as e:
        session.rollback()
        await interaction.followup.send(f"❌ Error: {e}")
    finally:
        session.close()

# ----- Undo -----
@bot.tree.command(name="undo", description="Undo your last report (within 10 minutes).")
async def undo(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    session = SessionLocal()
    try:
        q = select(Match).where(Match.voided == False).order_by(Match.id.desc())
        if not is_admin(interaction.user.id):
            q = q.where(
                Match.reporter_id == interaction.user.id,
                Match.played_at >= now_utc() - timedelta(minutes=10)
            )
        m = session.execute(q).scalars().first()
        if not m:
            return await interaction.followup.send("Nothing eligible to undo.", ephemeral=True)
        m.voided = True
        session.add(AuditLog(who_id=interaction.user.id, action=f"undo match {m.id}"))
        session.commit()
        await interaction.followup.send(f"↩️ Voided match `{m.id}`.", ephemeral=True)
    except Exception as e:
        session.rollback()
        await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    finally:
        session.close()

# Public: /help
@bot.tree.command(name="help", description="How to use the scoreboard bot")
async def help_cmd(interaction: discord.Interaction):
    text = (
        "## 🏆 Scoreboard Bot Help\n"
        "**Track head-to-head games, keep records, and run seasons in your server.**\n"
        "Use slash commands (`/command`).\n\n"

        "### 🎮 Match Commands\n"
        "• **/report** `game:<name> winner:@User loser:@User [score_w] [score_l] [season]`\n"
        "  Record a result. Example:\n"
        "  `/report game:madden winner:@Key loser:@Cam score_w:21 score_l:17 season:Fall2025`\n"
        "• **/record** `[game] [user] [vs] [season]` — player record, head-to-head, or top list\n"
        "• **/head2head** `user1:@User user2:@User [game] [season]` — quick H2H\n"
        "• **/leaderboard** `[game] [season]` — top 10 by wins (ties break by losses, then win%)\n\n"

        "### 🧹 Admin Utilities\n"
        "• **/undo** — players: undo your last report (10 min). Admins: undo latest match.\n"
        "• **/matchup_reset** `user1:@User user2:@User game:<name> [season]` — reset a rivalry to **0–0** for a game.\n\n"

        "### 📅 Seasons (Admin Only)\n"
        "• **/season_start** `name:<name> [game]`\n"
        "• **/season_end** `name:<name>`\n"
        "• **/season_reset** `name:<name>`\n\n"

        "### ℹ️ Notes\n"
        "• Match, stats, **and matchup resets** post **publicly**.\n"
        "• Game names are case/whitespace-insensitive (e.g., `Madden`, `madden`, `M a d d e n` are the same).\n"
        "• Add `[game]` and/or `[season]` to narrow stats (e.g., `game:madden season:Fall2025`).\n"
        "• Admins are the user IDs in the bot config (`ADMINS`).\n"
    )
    await interaction.response.send_message(text)

# ----- Run -----
def _looks_like_bot_token(t: str) -> bool:
    return isinstance(t, str) and t.count(".") == 2 and len(t) > 50

if __name__ == "__main__":
    if not TOKEN or not _looks_like_bot_token(TOKEN):
        raise RuntimeError("DISCORD_TOKEN seems invalid or missing. Use the Bot tab token (three dot-separated parts).")
    bot.run(TOKEN)
