# Deploy — VPS Ubuntu 22.04

Setup completo da VPS Hostinger em ~5 min, com um único script idempotente.

## Primeira vez (setup)

Do seu Windows (PowerShell ou Git Bash), dentro da pasta do projeto:

```bash
# 1) Copia o script pro servidor
scp "deploy/vps-setup.sh" root@2.24.208.238:/root/vps-setup.sh

# 2) Roda no servidor
ssh root@2.24.208.238 "bash /root/vps-setup.sh"
```

O script faz:

1. `apt update && upgrade` não-interativo
2. Instala `curl, git, nginx, ufw, fail2ban, unattended-upgrades`
3. Instala Node.js 20 LTS (NodeSource)
4. Cria usuário `deploy` (sudo sem senha p/ operação, copia as authorized_keys do root)
5. `git clone` do repositório GitHub
6. `npm install --omit=dev`
7. Grava `.env` de produção (chmod 600, dono = deploy)
8. Instala PM2 global, starta o app como serviço systemd (boot automático)
9. Configura Nginx (proxy `:80 → 127.0.0.1:3000`) com logs dedicados
10. Liga UFW (22/80/443), fail2ban, unattended-upgrades
11. Valida que o app responde

Após rodar, acesse: **http://2.24.208.238**

## Redeploys (após `git push`)

```bash
ssh root@2.24.208.238 "sudo -u deploy bash /home/deploy/agente-marketplace-am/deploy/vps-update.sh"
```

Ou diretamente no servidor:
```bash
cd /home/deploy/agente-marketplace-am && bash deploy/vps-update.sh
```

## Comandos úteis no servidor

| Ação | Comando |
|---|---|
| Status do app | `sudo -u deploy pm2 status` |
| Logs em tempo real | `sudo -u deploy pm2 logs agente-am` |
| Reiniciar app | `sudo -u deploy pm2 restart agente-am` |
| Parar app | `sudo -u deploy pm2 stop agente-am` |
| Testar config Nginx | `sudo nginx -t` |
| Recarregar Nginx | `sudo systemctl reload nginx` |
| Status firewall | `sudo ufw status verbose` |
| Status fail2ban | `sudo fail2ban-client status sshd` |
| Editar .env | `sudo -u deploy nano /home/deploy/agente-marketplace-am/.env` |
| Logs Nginx | `sudo tail -f /var/log/nginx/agente-am.error.log` |

## SSL/HTTPS (quando tiver domínio)

```bash
# Apontar DNS A do domínio para 2.24.208.238, aguardar propagar, depois:
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d seudominio.com.br --non-interactive --agree-tos -m seu@email.com
```

Depois, atualize `ML_REDIRECT_URI` no `.env` e no ML DevCenter para `https://seudominio.com.br/callback`.

## Segurança pós-setup (opcional)

Depois de confirmar que o `deploy` consegue entrar por SSH:

```bash
# Desabilita login root por senha
sudo sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo systemctl restart sshd
```

## Arquivos

- `vps-setup.sh` — **contém segredos** (ML_CLIENT_SECRET) — ignorado pelo git, só existe localmente
- `vps-update.sh` — seguro, vai pro git
- `README.md` — este arquivo
