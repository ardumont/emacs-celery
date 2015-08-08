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

;; o.s. pre-requisite:
;; - either an accessible remote celery ready machine
;; - either your local celery ready machine

;; For example, either the local machine with:
;; - celery installed on machine (apt-get install -y celeryd)
;; - A ssh ready machine (cf. README.org for detailed example)
;;
;; If using ssh, configure this mode to know it:
;; (custom-set-variables '(celery-command "ssh remote-node celery"))
;; and you should be good to go.
;;
;; You can order or filter the celery data outputed per worker using
;; `celery-workers-list':
;; (custom-set-variables '(celery-workers-list '(aw01 aw02)))
;;

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

(defcustom celery-workers-list nil
  "If non nil, filter the stats according to the content of this list.
This is a list of worker names.")

(defvar celery-last-known-stats nil
  "Latest worker stats.
Mostly to work offline.")

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

(defun celery-count-processes-per-worker (stats worker)
  "Compute the number of tasks from STATS per WORKER."
  (-when-let (w (assoc-default worker stats))
    (->> w
         (assoc-default 'pool)
         (assoc-default 'processes)
         length)))

(defun celery-all-worker-names (stats)
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

(defun celery--to-org-table-row (stats)
  "Compute a row string from the STATS."
  (->> (mapcar (-compose #'int-to-string (-partial #'celery-total-tasks-per-worker stats))
               (celery-all-worker-names stats))
       (cons (s-trim (current-time-string)))
       (s-join " | " )
       (format "| %s | ")))

(defun celery--stats-to-org-row (stats)
  "Dump an org table row to the current buffer from STATS."
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
      (insert (celery--to-org-table-row stats))
      ;; align org column
      (org-cycle)
      ;; recompute eventual formula
      (org-table-recalculate 'all))))

(defun celery-simplify-stats (stats)
  "Compute the number of total tasks done per worker from the STATS."
  (mapcar (-juxt 'identity
                 (-compose (-partial #'cons :total) (-partial #'celery-total-tasks-per-worker stats))
                 (-compose (-partial #'cons :processes) (-partial #'celery-count-processes-per-worker stats)))
          (celery-all-worker-names stats)))

(defun celery-filter-workers (stats &optional filter-workers-list)
  "Filter the STATS according to FILTER-WORKERS-LIST.
If filter is nil, keep the STATS as is."
  (if filter-workers-list
      (mapcar (-rpartial 'assq stats) filter-workers-list)
    stats))

(defun celery--compute-stats-workers-with-refresh (&optional refresh)
  "If REFRESH is specified or no previous stats, trigger a computation.
Otherwise, reuse the latest known values."
  (if (or refresh (null celery-last-known-stats))
      (setq celery-last-known-stats (celery-compute-full-stats-workers))
    celery-last-known-stats))

(defun celery--with-delay-apply (fn &optional refresh)
  "Execute FN which takes a simplified STATS parameter.
Detail:
if REFRESH is non nil or no known stats exists, trigger a computation
and store the result in `celery-last-known-stats for later.
Otherwise, reuse the latest known stats `celery-last-known-stats'.
Then simplify data to keep only relevant data (at the moment).
Then filter data according to celery-workers-list.
Then execute FN to do thy bidding."
  (deferred:$
    (deferred:call (-partial 'celery--compute-stats-workers-with-refresh refresh))
    (deferred:nextc it 'celery-simplify-stats)
    (deferred:nextc it (-rpartial 'celery-filter-workers celery-workers-list))
    (deferred:nextc it fn)))

;;;###autoload
(defun celery-stats-to-org-row (&optional refresh)
  "Compute simplified stats with optional REFRESH.
if REFRESH is non nil or no known stats exists, trigger a computation.
Otherwise, reuse the latest known values.
Also, use `celery-workers-list' to order/filter celery output.
Otherwise, reuse the latest known stats `celery-last-known-stats'.
This command writes a dummy formatted org-table row.
So this needs to be applied in an org context to make sense."
  (interactive "P")
  (celery--with-delay-apply 'celery--stats-to-org-row refresh))

;;;###autoload
(defun celery-compute-stats-workers (&optional refresh)
  "Compute the simplified workers' stats.
if REFRESH is non nil, trigger a computation.
Otherwise, reuse the latest known values."
  (interactive "P")
  (celery--with-delay-apply (-partial 'celery-log "Stats: %s") refresh))

(defun celery-all-tasks-consumed (stats)
  "Compute the total number of consumed tasks from the STATS."
  (->> stats
       (mapcar (-partial 'assoc-default :total))
       (apply #'+)))

;;;###autoload
(defun celery-compute-tasks-consumed-workers (&optional refresh)
  "Check the current number of tasks executed by workers in celery.
if REFRESH is mentioned, trigger a check, otherwise, use the latest value."
  (interactive "P")
  (celery--with-delay-apply
   (-compose (-partial 'celery-log "Number of total tasks done: %s") 'celery-all-tasks-consumed)
   refresh))

(defvar celery-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c , s") 'celery-compute-stats-workers)
    (define-key map (kbd "C-c , o") 'celery-stats-to-org-row)
    (define-key map (kbd "C-c , a") 'celery-compute-tasks-consumed-workers)
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
