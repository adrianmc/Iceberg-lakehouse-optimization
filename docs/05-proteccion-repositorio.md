# Protección del repositorio — solo el mantenedor puede hacer commit

Objetivo: que **cualquiera pueda descargar y leer** el asset, pero que **solo el mantenedor
pueda escribir** en la rama `main`.

---

## Lo primero: qué bloquea realmente cada mecanismo

Hay confusión frecuente sobre esto. Los mecanismos **no son equivalentes**.

| Mecanismo | ¿Bloquea commits? | Qué hace realmente |
|---|:--:|---|
| **Permisos de colaborador** | **Sí** | Control primario. Sin permiso `Write`, el `git push` es rechazado por el servidor. |
| **Ruleset — Restrict updates** | **Sí** | Solo quienes tengan *bypass* pueden hacer push a las ramas que coincidan con el patrón. |
| **Branch protection — Restrict who can push** | **Sí** | Equivalente clásico. Requiere repo propiedad de una **organización**. |
| **Branch protection — Require PR** | Parcial | Obliga a pasar por PR, pero quien tenga `Write` sigue pudiendo aprobar y mergear. |
| **`CODEOWNERS`** | **No** | Solo enruta revisiones. **No bloquea nada por sí mismo.** Requiere activar *Require review from Code Owners* en la regla de protección para tener efecto. |

> **No confiar en `CODEOWNERS` como control de acceso.** Es documentación y enrutamiento de
> revisores. Sin una regla de protección que lo exija, no impide ningún push.

---

## Capa 1 — Permisos de colaborador (control primario)

Es el mecanismo más robusto y **no depende del plan de GitHub**.

### Repositorio bajo cuenta personal

Por defecto solo el propietario tiene escritura. Para que otros consuman el asset:

- **Repo público** → cualquiera puede leer y hacer fork. No agregar colaboradores.
- **Repo privado** → agregar personas con rol **Read** únicamente.

```
Settings → Collaborators → Add people → rol: Read
```

Con rol `Read`, el `git push` se rechaza en el servidor. Pueden clonar, descargar releases,
hacer fork y abrir pull requests.

### Repositorio bajo una organización

El propietario de la organización puede sobrescribir estas decisiones. Configurar:

```
Settings → Collaborators and teams
  · Base role de la organización  → Read
  · Equipos con acceso            → Read
  · Tu usuario                    → Admin
```

Verificar además que **Base permissions** de la organización no otorgue `Write` por defecto:

```
Organization Settings → Member privileges → Base permissions → Read
```

---

## Capa 2 — Ruleset sobre `main` (defensa en profundidad)

Protege contra escalamientos accidentales de permisos y contra force-push del propio
mantenedor.

### Disponibilidad por plan

Según documentación de GitHub:

- Rulesets: disponibles en repos **públicos** con GitHub Free, y en repos **públicos y
  privados** con GitHub Pro, Team y Enterprise Cloud.
- Branch protection clásica: misma matriz de planes.
- *Restrict who can push* (branch protection clásica): requiere repos propiedad de una
  **organización** en Team / Enterprise Cloud / Enterprise Server.

> Si el repo es **privado bajo cuenta personal con plan Free**, los rulesets no están
> disponibles. En ese escenario la Capa 1 (permisos `Read`) es suficiente y es el control
> efectivo. Alternativa sin costo: hacer el repositorio **público**.

### Configuración por interfaz

```
Settings → Rules → Rulesets → New ruleset → New branch ruleset
```

| Campo | Valor |
|---|---|
| Ruleset Name | `protect-main` |
| Enforcement status | `Active` |
| Bypass list | Agregar **solo tu usuario** (o `Repository admin`) |
| Target branches | `Include default branch` |

Reglas a activar:

- [x] **Restrict updates** — solo quienes tengan bypass pueden hacer push
- [x] **Restrict deletions** — nadie puede borrar la rama
- [x] **Block force pushes** — impide reescribir historia
- [x] **Require a pull request before merging**
  - Required approvals: `1`
  - [x] Require review from Code Owners
- [x] **Require status checks to pass** → seleccionar `validate` (workflow incluido)
- [ ] Require signed commits *(opcional — ver más abajo)*

### Configuración por API

```bash
gh api --method POST /repos/{owner}/{repo}/rulesets \
  --input - << 'EOF'
{
  "name": "protect-main",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": { "include": ["~DEFAULT_BRANCH"], "exclude": [] }
  },
  "bypass_actors": [
    { "actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always" }
  ],
  "rules": [
    { "type": "update" },
    { "type": "deletion" },
    { "type": "non_fast_forward" },
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 1,
        "require_code_owner_review": true,
        "dismiss_stale_reviews_on_push": true,
        "require_last_push_approval": false,
        "required_review_thread_resolution": true
      }
    }
  ]
}
EOF
```

> `actor_id: 5` corresponde al rol `Repository admin`. Verificar los IDs de rol vigentes en
> el repositorio antes de aplicar:
> `gh api /repos/{owner}/{repo}/rulesets/rule-suites` y la documentación de la API de rulesets.

---

## Capa 3 — `CODEOWNERS`

Complemento de la Capa 2, **no sustituto**. Surte efecto únicamente cuando el ruleset tiene
activado *Require review from Code Owners*.

Archivo en `.github/CODEOWNERS`:

```
* @tu-usuario
```

Con esto, cualquier PR que toque cualquier archivo requiere tu aprobación explícita.

---

## Capa 4 — Firma de commits (opcional)

Garantiza que los commits en `main` provienen de una clave que controlas.

```bash
# Generar clave de firma
ssh-keygen -t ed25519 -C "firma-commits" -f ~/.ssh/git_signing

# Configurar git
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/git_signing.pub
git config --global commit.gpgsign true
```

Registrar la clave pública en `Settings → SSH and GPG keys → New SSH key → tipo: Signing key`.

Luego activar en el ruleset: **Require signed commits**.

> Advertencia: si se activa esta regla, los merges hechos desde la interfaz web de GitHub
> quedan firmados por GitHub, no por ti. Validar el flujo antes de hacerla obligatoria.

---

## Distribución del asset sin otorgar escritura

### Releases

La forma recomendada de distribuir. Genera un ZIP versionado e inmutable:

```bash
git tag -a v1.0.0 -m "Estándar Iceberg v2 Telco — versión inicial"
git push origin v1.0.0

gh release create v1.0.0 \
  --title "v1.0.0 — Estándar Iceberg v2 Telco" \
  --notes-file CHANGELOG.md
```

Enlace de descarga directa:

```
https://github.com/<owner>/<repo>/archive/refs/tags/v1.0.0.zip
```

### Protección de tags

Evita que se sobrescriban releases publicadas:

```
Settings → Rules → Rulesets → New tag ruleset
  Target tags       : v*
  Restrict updates  : ✔
  Restrict deletions: ✔
  Bypass            : solo tu usuario
```

---

## Verificación

Comprobar que la configuración quedó efectiva:

```bash
# Reglas activas sobre main
gh api /repos/{owner}/{repo}/rules/branches/main

# Colaboradores y sus permisos
gh api /repos/{owner}/{repo}/collaborators \
  --jq '.[] | {login: .login, permission: .role_name}'

# Rulesets configurados
gh api /repos/{owner}/{repo}/rulesets
```

Prueba funcional recomendada: pedir a un colega con acceso `Read` que intente
`git push` sobre `main`. Debe recibir rechazo del servidor.

---

## Resumen de la configuración objetivo

| Elemento | Valor |
|---|---|
| Visibilidad del repo | Público (o privado con colaboradores en `Read`) |
| Base permission de la org | `Read` |
| Tu rol | `Admin` |
| Resto de usuarios | `Read` |
| Ruleset `protect-main` | Activo, bypass solo para ti |
| `CODEOWNERS` | `* @tu-usuario` |
| Flujo de contribución | Fork → PR → tu aprobación → merge |
| Distribución | Releases con tags protegidos |
