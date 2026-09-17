# PlayPulse — Ansible production deployment

Automated deployment of PlayPulse to a **production Ubuntu server**.

Ansible here is a **thin layer on top of the project's own tooling**. It does not
reimplement any Compose or bring-up logic — it installs Docker, fetches the repo,
renders `.env`, and then runs the existing scripts:

- `scripts/run_tests_in_docker.sh` — optional isolated suite (**before** bring-up)
- `scripts/bring_up.sh` — prepare env files, staged Compose up, wait for health
- `scripts/infra/check_infra.sh` — smoke-test Postgres / Kafka / Metabase

## What it does

| Role            | Action |
|-----------------|--------|
| `docker`        | Install Docker Engine + Compose plugin from Docker's official APT repo (idempotent) |
| `project_setup` | `git clone`/`pull` the repo into `project_dir`, then render `.env` from `templates/env.j2` (secrets from Vault) |
| `deploy`        | Optionally run `run_tests_in_docker.sh`, then `bring_up.sh --skip-check`, then `check_infra.sh` |

## Layout

```
ansible/
  ansible.cfg                 # config: inventory, roles path, diff, sudo
  inventory.ini.example       # copy to inventory.ini and add the real server IP
  playbook.yml                # the play (docker -> project_setup -> deploy)
  group_vars/
    all.yml                   # non-sensitive vars (repo URL, project_dir, ports)
    vault.yml.example         # template for the encrypted secrets file
  roles/
    docker/tasks/main.yml
    project_setup/tasks/main.yml
    deploy/tasks/main.yml
  templates/
    env.j2                    # Jinja2 .env (mirrors the root .env.example)
```

`inventory.ini` (the real one) and `group_vars/vault.yml` (the real, encrypted
secrets) are **gitignored** and must never be committed — only the `.example`
files are tracked.

---

## 1. Install Ansible on the control node

The **control node** is your laptop/CI box, not the server. Pick one:

```bash
# Ubuntu/Debian
sudo apt update && sudo apt install -y ansible

# Any OS with Python (pipx recommended)
pipx install --include-deps ansible
# or
python3 -m pip install --user ansible
```

Verify:

```bash
ansible --version
```

The **target server** only needs SSH access and a sudo-capable user — Ansible
installs everything else (including Docker).

## 2. Create `inventory.ini` from the example

```bash
cd ansible
cp inventory.ini.example inventory.ini
```

Edit `inventory.ini` and set your server's **real** IP/hostname and SSH user:

```ini
[production]
prod-server ansible_host=YOUR.SERVER.IP.HERE

[production:vars]
ansible_user=ubuntu
# ansible_ssh_private_key_file=~/.ssh/id_ed25519
```

Check connectivity:

```bash
ansible -i inventory.ini production -m ping
```

## 3. Create the real, encrypted `vault.yml`

The example shows the required keys. Create the **encrypted** real file with
`ansible-vault` (you will be asked to set a vault password):

```bash
ansible-vault create group_vars/vault.yml
```

Paste your real production Postgres values:

```yaml
vault_postgres_db: "playpulse_prod"
vault_postgres_user: "playpulse"
vault_postgres_password: "a-long-random-secret"
```

Useful later:

```bash
ansible-vault edit group_vars/vault.yml     # change values
ansible-vault view group_vars/vault.yml     # read without editing
```

Set any non-sensitive values (repo URL, `project_dir`, ports) in
`group_vars/all.yml`.

## 4. Run the playbook

### Normal deploy (no full docker test suite)

```bash
ansible-playbook -i inventory.ini playbook.yml --ask-vault-pass
```

`--ask-vault-pass` prompts for the vault password so Ansible can decrypt
`group_vars/vault.yml`. Add `-K` (`--ask-become-pass`) if your sudo needs a
password.

### Deploy with isolated docker tests first

Runs `scripts/run_tests_in_docker.sh` on the server **before** `bring_up.sh`
(uses the separate `playpulse_test` stack and `.env.test`; does not touch the
live root `.env`). Default is off — enable deliberately:

```bash
ansible-playbook -i inventory.ini playbook.yml --ask-vault-pass \
  -e "run_tests_before_deploy=true"
```

Why optional: the suite includes live / oracle cases that may be **SKIPPED**
when the host has no outbound Play Store access or no `tshark`. Those are
environmental, not deploy bugs. The Ansible task does not fail the whole
playbook on a non-zero test exit; it prints the summary and a warning so you
decide. Real `FAIL` rows still deserve investigation before trusting the box.

Order on the server:

1. (optional) `run_tests_in_docker.sh`
2. `bring_up.sh --skip-check`
3. `check_infra.sh`

---

## Running only part of the playbook (tags)

Every task is tagged. Run a subset with `--tags`:

```bash
# Only update the code and redeploy — does NOT reinstall Docker.
# (git pull + re-render .env + optional tests + bring_up + check_infra)
ansible-playbook -i inventory.ini playbook.yml --tags deploy --ask-vault-pass

# Only the optional docker test suite (still needs run_tests_before_deploy=true)
ansible-playbook -i inventory.ini playbook.yml --tags tests --ask-vault-pass \
  -e "run_tests_before_deploy=true"

# Only (re)install Docker Engine + Compose plugin.
ansible-playbook -i inventory.ini playbook.yml --tags docker --ask-vault-pass

# Only fetch code + render .env (no deploy).
ansible-playbook -i inventory.ini playbook.yml --tags project_setup --ask-vault-pass
```

| Tag             | Runs |
|-----------------|------|
| `docker`        | Docker Engine + Compose install |
| `project_setup` | git clone/pull + `.env` render + git install |
| `deploy`        | git clone/pull + `.env` render + optional tests + `bring_up` + `check_infra` |
| `tests`         | only the optional `run_tests_in_docker.sh` block (requires `-e run_tests_before_deploy=true`) |

## The `.env` diff

`templates/env.j2` renders to `<project_dir>/.env` on the server. The `template`
module creates it if missing, and when the content changes it reports `changed`
and prints a unified **diff** of what changed (diff is enabled by default via
`ansible.cfg`, or pass `--diff`). A timestamped backup of the previous `.env` is
kept on every change.

## Validate without a server

```bash
# Syntax only
ansible-playbook -i inventory.ini playbook.yml --syntax-check

# Dry run (no changes made). Needs a readable vault.yml.
ansible-playbook -i inventory.ini playbook.yml --check --diff --ask-vault-pass
```

## Notes

- The health check runs `scripts/infra/check_infra.sh` (its real path in this
  repo), and `bring_up.sh` is invoked with `--skip-check` so the smoke test is a
  single, clearly-reported step rather than running twice.
- Optional docker tests run **before** bring-up so they do not compete with the
  live stack and so a broken suite is visible before services start. Variable:
  `run_tests_before_deploy` in `group_vars/all.yml` (default `false`).
- The playbook runs as root via `become` (set in `ansible.cfg`); the login user
  is also added to the `docker` group for convenient manual `docker` use.
- Idempotency: on a second full run the `docker` and `project_setup` roles report
  no changes. The `deploy` role executes shell scripts, so Ansible often marks
  those tasks `changed`, but the underlying `docker compose up -d` is idempotent
  and `check_infra.sh` passes on every run.
