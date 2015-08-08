;;; celery.el --- a minor mode to draw stats from celery and more?  -*- lexical-binding: t; -*-

;; Copyright (C) 2015  ardumont

;; Author: ardumont <eniotna.t@gmail.com>
;; Keywords: celery, convenience
;; Package-Requires: ((emacs "24") (dash-functional "2.11.0") (s "1.9.0") (deferred "0.3.2"))
;; Version: 0.0.2
;; URL: https://github.com/ardumont/emacs-celery

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:

;; Install this package from buffer (this will fetch the needed emacs deps)
;; M-x package-install-from-buffer RET

;; os pre-requisite:
;; - either an accessible remote celery ready machine
;; - either your local machine celery ready

;; For example, either the local machine with:
;; - celery installed on machine (apt-get install -y celeryd)
;; - A ssh ready machine (cf. README.org for detailed):
;; Now configure or access to a remote machine already setuped for it and the
;; client machine able to connect to such machine:
;; (custom-set-variables '(celery-command "ssh remote-node celery"))
;; and you are good to go.

;;; Code:

(require 'deferred)
(require 'dash-functional)
(require 'json)
(require 's)

(defcustom celery-command "celery"
  "The celery command in charge of outputing the result this mode parse.
The user can override this.
For example, if a remote machine only knows celery, it could be defined as:
\(custom-set-variables '\(celery-command \"ssh remote-node celery\"\)\)")

(defun celery-log (&rest strs)
  "Log STRS."
  (apply #'message (format "Celery - %s" (car strs)) (cdr strs)))

(defun celery--compute-raw-celery-output ()
  "Execute the celery command and return the raw output."
  (-> (format "%s inspect stats --quiet --no-color" celery-command)
      shell-command-to-string ))

(defun celery--compute-json-string-stats ()
  "Compute the workers' stats as json string."
  (let ((initial-json (with-temp-buffer
                        (insert (celery--compute-raw-celery-output))
                        (goto-char (point-min))
                        (while (re-search-forward "celery@\\(.*\\): OK" nil t)
                          (replace-match (format ", \"%s\":" (match-string 1))))
                        (buffer-substring-no-properties (point-min) (point-max)))))
    (--> initial-json
         (s-chop-prefix "," it)
         (format "{\n%s\n}" it))))

(defun celery-compute-full-stats-workers ()
  "Compute the worker' stats in json data structure."
  (let ((stats-json-str-output (celery--compute-json-string-stats)))
    (with-temp-buffer
      (insert stats-json-str-output)
      (goto-char (point-min))
      (json-read))))

(defvar celery-last-known-stats nil
  "Latest worker stats.")

(defun celery-count-processes-per-worker (stats worker)
  "Compute the number of tasks from STATS per WORKER."
  (-when-let (w (assoc-default worker stats))
    (->> w
         (assoc-default 'pool)
         (assoc-default 'processes)
         length)))

(defun celery-all-workers (stats)
  "Compute the number of workers from STATS."
  (mapcar #'car stats))

;; total for one worker
(defun celery-total-tasks-per-worker (stats worker)
  "Compute the total number of tasks from STATS for WORKER."
  (-when-let (w (assoc-default worker stats))
    (->> w
         (assoc-default 'total)
         car
         cdr)))

(defun celery--to-org-table-row (stats &optional workers)
  "Compute a row string from the STATS.
If WORKERS is specified, use such list otherwise, use the workers from the stats."
  (->> (mapcar (-compose #'int-to-string (-partial #'celery-total-tasks-per-worker stats))
               (if workers workers (celery-all-workers stats)))
       (cons (s-trim (current-time-string)))
       (s-join " | " )
       (format "| %s | ")))

(defun celery--stats-to-org-row (stats &optional workers)
  "Dump an org table row to the current buffer from STATS and optional WORKERS.
If WORKERS list is specified, use it otherwise, use the workers list in STATS."
  (save-excursion
    (with-current-buffer (current-buffer)
      ;; make sure i'm at the right position
      (beginning-of-line)
      (goto-char (+ 2 (point-at-bol)))
      ;; insert a new org row
      (call-interactively 'org-return)
      (beginning-of-line)
      ;; clean up
      (kill-line)
      ;; insert the information we look for
      (insert (celery--to-org-table-row stats workers))
      ;; align org column
      (org-cycle)
      ;; recompute eventual formula
      (org-table-recalculate 'all))))

(defun celery-simplify-stats (stats)
  "Compute the number of total tasks done per worker from the STATS."
  (mapcar (-juxt 'identity
                 (-compose (-partial #'cons :total) (-partial #'celery-total-tasks-per-worker stats))
                 (-compose (-partial #'cons :processes) (-partial #'celery-count-processes-per-worker stats)))
          (celery-all-workers stats)))

(defun celery--compute-stats-workers-with-refresh (&optional refresh)
  "If REFRESH is specified or no previous stats, trigger a computation.
Otherwise, reuse the latest known values."
  (if (or refresh (null celery-last-known-stats))
      (setq celery-last-known-stats (celery-compute-full-stats-workers))
    celery-last-known-stats))

(defun celery--with-delay-apply (fn &optional refresh)
  "Execute FN which takes a simplified STATS parameter.
But first, compute the global stats.
if REFRESH is non nil, trigger the global stats computation (refresh).
Otherwise, reuse the latest known stats `celery-last-known-stats'."
  (deferred:$
    (deferred:call (-partial 'celery--compute-stats-workers-with-refresh refresh))
    (deferred:nextc it 'celery-simplify-stats)
    (deferred:nextc it fn)))

(defcustom celery-workers-list
  '(awork01.cloudapp.net
    awork02.cloudapp.net
    awork03.cloudapp.net
    awork04.cloudapp.net
    worker01
    worker02
    worker03
    worker04
    worker05
    worker06
    worker07
    worker08)
  "The list of workers to extract stats from.")

;;;###autoload
(defun celery-stats-to-org-row (&optional refresh workers)
  "Compute simplified stats with optional REFRESH for WORKERS.
if REFRESH is non nil or no stats exists, trigger a computation.
Otherwise, reuse the latest known values.
Also, if workers is specified, use this list otherwise use
`celery-workers-list'.
This command writes a dummy formatted org-table row.
So this needs to be applied in an org context to make sense."
  (interactive "P")
  (celery--with-delay-apply
   (lambda (stats)
     (celery--stats-to-org-row stats
                               (if workers workers celery-workers-list)))
   refresh))

;;;###autoload
(defun celery-compute-stats-workers (&optional refresh)
  "Compute the simplified workers' stats.
if REFRESH is non nil, trigger a computation.
Otherwise, reuse the latest known values."
  (interactive "P")
  (celery--with-delay-apply
   (-partial 'celery-log "Stats: %s")
   refresh))

(defun celery-number-tasks-consumed-per-worker (stats)
  "Compute number tasks consumed per worker from the STATS."
  (->> stats
       celery-simplify-stats
       (mapcar #'cadr)
       (apply #'+)))

;;;###autoload
(defun celery-check-cloner-workers (&optional refresh)
  "Check the current number of tasks executed by workers in celery.
if REFRESH is mentioned, trigger a check, otherwise, use the latest value."
  (interactive "P")
  (celery--with-delay-apply
   (-partial 'celery-log "Number of tasks done: %s")
   refresh))

(defvar celery-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c , s") 'celery-compute-stats-workers)
    (define-key map (kbd "C-c , o") 'celery-stats-to-org-row)
    (define-key map (kbd "C-c , a") 'celery-check-cloner-workers)
    map)
  "Keymap for celery mode.")

;;;###autoload
(define-minor-mode celery-mode
  "Minor mode to consolidate Emacs' celery extensions.

\\{celery-mode-map}"
  :lighter " σ"
  :keymap celery-mode-map)

(provide 'celery)
;;; celery.el ends here
